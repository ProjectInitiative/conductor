package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	conductorv1alpha1 "github.com/projectinitiative/conductor/pkg/apis/conductor/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	modelDownloadLabel            = "conductor.projectinitiative.io/model-download"
	modelDownloadOwner            = "conductor.projectinitiative.io/model-download-uid"
	modelDownloadProgressInterval = 10 * time.Second
	modelDownloadMountPath        = "/models"

	ProgressFilesCompleted              = "conductor.projectinitiative.io/files-completed"
	ProgressFilesTotal                  = "conductor.projectinitiative.io/files-total"
	ProgressBytesPersisted              = "conductor.projectinitiative.io/bytes-persisted"
	ProgressInstantaneousBytesPerSecond = "conductor.projectinitiative.io/instantaneous-bytes-per-second"
	ProgressAverageBytesPerSecond       = "conductor.projectinitiative.io/average-bytes-per-second"
)

// ModelDownloadReconciler creates and observes exactly one typed downloader Job
// for each ModelDownload. It never applies an arbitrary manifest.
type ModelDownloadReconciler struct {
	client.Client
	Clock clock.Clock
}

// ModelDownloadJobName returns a stable DNS label for the one child Job.
func ModelDownloadJobName(downloadName string) string {
	name := strings.ToLower(downloadName) + "-download"
	if len(name) <= 63 {
		return name
	}
	// Keep the common case readable while making truncation collision-resistant.
	return strings.TrimRight(name[:54], "-") + "-" + shortHash(downloadName)
}

func shortHash(value string) string {
	var hash uint32 = 2166136261
	for i := 0; i < len(value); i++ {
		hash = (hash ^ uint32(value[i])) * 16777619
	}
	return fmt.Sprintf("%08x", hash)
}

// BuildDownloaderEnv encodes the typed API into the downloader's stable env
// interface. JSON is used for model lists so repositories, revisions, and
// include patterns cannot be ambiguous or shell-interpreted.
func BuildDownloaderEnv(download *conductorv1alpha1.ModelDownload, class *conductorv1alpha1.DownloaderClass) ([]corev1.EnvVar, error) {
	models := make([]string, 0, len(download.Spec.Models))
	for _, model := range download.Spec.Models {
		if model.Repository == "" || model.Revision == "" {
			return nil, fmt.Errorf("each model requires repository and revision")
		}
		spec := model.Repository + "@" + model.Revision
		if len(model.Include) > 0 {
			spec += "#" + strings.Join(model.Include, "|")
		}
		models = append(models, spec)
	}
	pull, deleteModels := "", ""
	switch download.Spec.Operation {
	case conductorv1alpha1.ModelDownloadEnsure:
		pull = strings.Join(models, ",")
	case conductorv1alpha1.ModelDownloadDelete:
		// The existing puller accepts repository IDs or cache directory names for
		// deletion. Revisions/includes are intentionally ignored for deletion.
		deleteModels = strings.Join(func() []string {
			out := make([]string, 0, len(download.Spec.Models))
			for _, model := range download.Spec.Models {
				out = append(out, model.Repository)
			}
			return out
		}(), ",")
	default:
		return nil, fmt.Errorf("unsupported operation %q", download.Spec.Operation)
	}
	interval := download.Spec.ProgressIntervalSeconds
	if interval <= 0 {
		interval = int32(modelDownloadProgressInterval / time.Second)
	}
	return []corev1.EnvVar{
		{Name: "METRICS_PORT", Value: strconv.FormatInt(int64(metricsPort(class)), 10)},
		{Name: "PULL_MODELS", Value: pull},
		{Name: "DELETE_MODELS", Value: deleteModels},
		{Name: "RATE", Value: strconv.FormatInt(download.Spec.RateLimit.BytesPerSecond, 10)},
		{Name: "MAX_FILE_WORKERS", Value: strconv.FormatInt(int64(download.Spec.Workers.FileWorkers), 10)},
		{Name: "MAX_REPO_WORKERS", Value: strconv.FormatInt(int64(download.Spec.Workers.RepositoryWorkers), 10)},
		{Name: "MAX_DELETE_WORKERS", Value: strconv.FormatInt(int64(download.Spec.Workers.DeleteWorkers), 10)},
		{Name: "PROGRESS_INTERVAL", Value: strconv.FormatInt(int64(interval), 10)},
		{Name: "RECEIPT_PATH", Value: download.Spec.Verification.ReceiptPath},
		{Name: "RECEIPT_REQUIRED", Value: strconv.FormatBool(download.Spec.Verification.Required)},
	}, nil
}

func (r *ModelDownloadReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var download conductorv1alpha1.ModelDownload
	if err := r.Get(ctx, req.NamespacedName, &download); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if r.Clock == nil {
		r.Clock = clock.RealClock{}
	}

	var class conductorv1alpha1.DownloaderClass
	if err := r.Get(ctx, types.NamespacedName{Name: download.Spec.DownloaderRef}, &class); err != nil {
		if apierrors.IsNotFound(err) {
			return r.setDownloadFailure(ctx, &download, "DownloaderClassNotFound", err.Error())
		}
		return ctrl.Result{}, err
	}
	env, err := BuildDownloaderEnv(&download, &class)
	if err != nil {
		return r.setDownloadFailure(ctx, &download, "InvalidSpec", err.Error())
	}

	jobName := ModelDownloadJobName(download.Name)
	var job batchv1.Job
	err = r.Get(ctx, types.NamespacedName{Namespace: download.Namespace, Name: jobName}, &job)
	if apierrors.IsNotFound(err) {
		job = desiredModelDownloadJob(&download, &class, env)
		if err := controllerutil.SetControllerReference(&download, &job, r.Scheme()); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, &job); err != nil {
			return ctrl.Result{}, err
		}
		return r.setDownloadStatus(ctx, &download, conductorv1alpha1.ModelDownloadPending, &job)
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if !metav1.IsControlledBy(&job, &download) {
		// A deterministic name is not permission to take over an unrelated Job.
		// A job carrying both identity labels is an explicit, safe adoption
		// handshake for a controller restart or pre-created child.
		if job.Labels[modelDownloadLabel] != download.Name || job.Labels[modelDownloadOwner] != string(download.UID) {
			return r.setDownloadFailure(ctx, &download, "JobOwnershipConflict", fmt.Sprintf("job %s is not owned by this ModelDownload", job.Name))
		}
		if err := controllerutil.SetControllerReference(&download, &job, r.Scheme()); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Update(ctx, &job); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	phase := modelDownloadPhase(&job)
	applyProgressAnnotations(&download.Status, &job)
	return r.setDownloadStatus(ctx, &download, phase, &job)
}

func metricsPort(class *conductorv1alpha1.DownloaderClass) int32 {
	if class.Spec.MetricsPort > 0 {
		return class.Spec.MetricsPort
	}
	return 9105
}

func desiredModelDownloadJob(download *conductorv1alpha1.ModelDownload, class *conductorv1alpha1.DownloaderClass, env []corev1.EnvVar) batchv1.Job {
	pullPolicy := class.Spec.ImagePullPolicy
	if pullPolicy == "" {
		pullPolicy = corev1.PullIfNotPresent
	}
	return batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: ModelDownloadJobName(download.Name), Namespace: download.Namespace,
			Labels: map[string]string{modelDownloadLabel: download.Name, modelDownloadOwner: string(download.UID)},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: pointer(int32(0)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{modelDownloadLabel: download.Name}},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					NodeSelector:       download.Spec.Cache.NodeSelector,
					ServiceAccountName: class.Spec.ServiceAccountName,
					Volumes:            []corev1.Volume{{Name: "model-cache", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: download.Spec.Cache.PVC}}}},
					Containers:         []corev1.Container{{Name: "downloader", Image: class.Spec.Image, ImagePullPolicy: pullPolicy, Env: env, Ports: []corev1.ContainerPort{{Name: "metrics", ContainerPort: metricsPort(class), Protocol: corev1.ProtocolTCP}}, VolumeMounts: []corev1.VolumeMount{{Name: "model-cache", MountPath: modelDownloadMountPath}}}},
				},
			},
		},
	}
}

func pointer(value int32) *int32 { return &value }

func modelDownloadPhase(job *batchv1.Job) conductorv1alpha1.ModelDownloadPhase {
	if job.Status.Failed > 0 {
		return conductorv1alpha1.ModelDownloadFailed
	}
	if job.Status.Succeeded > 0 {
		return conductorv1alpha1.ModelDownloadSucceeded
	}
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return conductorv1alpha1.ModelDownloadFailed
		}
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return conductorv1alpha1.ModelDownloadSucceeded
		}
	}
	if job.Status.Active > 0 {
		return conductorv1alpha1.ModelDownloadRunning
	}
	return conductorv1alpha1.ModelDownloadPending
}

func applyProgressAnnotations(status *conductorv1alpha1.ModelDownloadStatus, job *batchv1.Job) {
	status.JobName = job.Name
	statusReceipt := func(key string, target *int64) {
		if value, err := strconv.ParseInt(job.Annotations[key], 10, 64); err == nil {
			*target = value
		}
	}
	statusReceipt(ProgressFilesCompleted, &status.FilesCompleted)
	statusReceipt(ProgressFilesTotal, &status.FilesTotal)
	statusReceipt(ProgressBytesPersisted, &status.BytesPersisted)
	statusReceipt(ProgressInstantaneousBytesPerSecond, &status.InstantaneousBytesPerSecond)
	statusReceipt(ProgressAverageBytesPerSecond, &status.AverageBytesPerSecond)
}

func (r *ModelDownloadReconciler) setDownloadFailure(ctx context.Context, download *conductorv1alpha1.ModelDownload, reason, message string) (ctrl.Result, error) {
	return r.setDownloadStatus(ctx, download, conductorv1alpha1.ModelDownloadFailed, nil, reason, message)
}

func (r *ModelDownloadReconciler) setDownloadStatus(ctx context.Context, download *conductorv1alpha1.ModelDownload, phase conductorv1alpha1.ModelDownloadPhase, job *batchv1.Job, failure ...string) (ctrl.Result, error) {
	previous := download.Status.DeepCopy()
	download.Status.ObservedGeneration = download.Generation
	download.Status.Phase = phase
	if job != nil {
		download.Status.JobName = job.Name
	}
	if download.Spec.Verification.ReceiptPath != "" {
		download.Status.ReceiptPath = download.Spec.Verification.ReceiptPath
	}
	now := metav1.NewTime(r.Clock.Now())
	condition := metav1.Condition{Type: "Ready", ObservedGeneration: download.Generation, LastTransitionTime: now}
	if phase == conductorv1alpha1.ModelDownloadSucceeded {
		condition.Status, condition.Reason = metav1.ConditionTrue, "JobSucceeded"
	} else if phase == conductorv1alpha1.ModelDownloadFailed {
		condition.Status, condition.Reason = metav1.ConditionFalse, "JobFailed"
	} else {
		condition.Status, condition.Reason = metav1.ConditionUnknown, "JobProgressing"
	}
	if len(failure) >= 2 {
		condition.Reason, condition.Message = failure[0], failure[1]
	}
	apimeta.SetStatusCondition(&download.Status.Conditions, condition)
	if equality.Semantic.DeepEqual(previous, &download.Status) {
		if phase == conductorv1alpha1.ModelDownloadPending || phase == conductorv1alpha1.ModelDownloadRunning {
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}
		return ctrl.Result{}, nil
	}
	if err := r.Status().Update(ctx, download); err != nil {
		return ctrl.Result{}, err
	}
	if phase == conductorv1alpha1.ModelDownloadPending || phase == conductorv1alpha1.ModelDownloadRunning {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func (r *ModelDownloadReconciler) mapDownloaderClass(ctx context.Context, obj client.Object) []reconcile.Request {
	var downloads conductorv1alpha1.ModelDownloadList
	if err := r.List(ctx, &downloads); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0)
	for i := range downloads.Items {
		if downloads.Items[i].Spec.DownloaderRef == obj.GetName() {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: downloads.Items[i].Namespace, Name: downloads.Items[i].Name}})
		}
	}
	return requests
}

func (r *ModelDownloadReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&conductorv1alpha1.ModelDownload{}).
		Owns(&batchv1.Job{}).
		Watches(&conductorv1alpha1.DownloaderClass{}, handler.EnqueueRequestsFromMapFunc(r.mapDownloaderClass)).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}
