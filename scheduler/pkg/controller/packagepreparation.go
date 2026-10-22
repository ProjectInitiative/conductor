package controller

import (
	"context"
	"fmt"
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
	packagePreparationLabel = "conductor.projectinitiative.io/package-preparation"
	packagePreparationOwner = "conductor.projectinitiative.io/package-preparation-uid"
	preparationMountPath    = "/models"
)

// PackagePreparationReconciler creates and observes exactly one typed
// preparation Job per PackagePreparation. It waits for an optional
// ModelDownload gate, then runs the PreparerClass image. It never applies an
// arbitrary manifest.
type PackagePreparationReconciler struct {
	client.Client
	Clock clock.Clock
}

// PackagePreparationJobName returns a stable DNS label for the one child Job.
func PackagePreparationJobName(preparationName string) string {
	name := strings.ToLower(preparationName) + "-prepare"
	if len(name) <= 63 {
		return name
	}
	return strings.TrimRight(name[:54], "-") + "-" + shortHash(preparationName)
}

// BuildPreparationEnv injects the stable paths contract for neutral preparer
// images. All paths are absolute inside the mounted cache.
func BuildPreparationEnv(preparation *conductorv1alpha1.PackagePreparation) []corev1.EnvVar {
	absolute := func(rel string) string {
		rel = strings.TrimPrefix(rel, "/")
		return preparationMountPath + "/" + rel
	}
	return []corev1.EnvVar{
		{Name: "INPUT_PATH", Value: absolute("raw")},
		{Name: "OUTPUT_PATH", Value: absolute(preparation.Spec.OutputPath)},
		{Name: "RECEIPT_PATH", Value: absolute(preparation.Spec.ReceiptPath)},
	}
}

func (r *PackagePreparationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var preparation conductorv1alpha1.PackagePreparation
	if err := r.Get(ctx, req.NamespacedName, &preparation); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if r.Clock == nil {
		r.Clock = clock.RealClock{}
	}

	// Optional dependency: the referenced ModelDownload must report Succeeded
	// before any preparation Job is created. This keeps "if not present, pull
	// first" declarative instead of racing the downloader on the same PVC.
	if ref := preparation.Spec.ModelDownloadRef; ref != "" {
		var download conductorv1alpha1.ModelDownload
		if err := r.Get(ctx, types.NamespacedName{Namespace: preparation.Namespace, Name: ref}, &download); err != nil {
			if apierrors.IsNotFound(err) {
				return r.setPreparationStatus(ctx, &preparation, conductorv1alpha1.PackagePreparationWaitingForDownload, nil, "ModelDownloadNotFound", "referenced ModelDownload "+ref+" does not exist yet")
			}
			return ctrl.Result{}, err
		}
		if download.Status.Phase != conductorv1alpha1.ModelDownloadSucceeded {
			return r.setPreparationStatus(ctx, &preparation, conductorv1alpha1.PackagePreparationWaitingForDownload, nil, "ModelDownloadIncomplete", "waiting for ModelDownload "+ref+" (phase "+string(download.Status.Phase)+")")
		}
	}

	var class conductorv1alpha1.PreparerClass
	if err := r.Get(ctx, types.NamespacedName{Name: preparation.Spec.PreparerRef}, &class); err != nil {
		if apierrors.IsNotFound(err) {
			return r.setPreparationFailure(ctx, &preparation, "PreparerClassNotFound", err.Error())
		}
		return ctrl.Result{}, err
	}
	if len(preparation.Spec.Args) == 0 {
		return r.setPreparationFailure(ctx, &preparation, "InvalidSpec", "args must contain the preparer entrypoint command")
	}

	jobName := PackagePreparationJobName(preparation.Name)
	var job batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Namespace: preparation.Namespace, Name: jobName}, &job)
	if apierrors.IsNotFound(err) {
		job = desiredPackagePreparationJob(&preparation, &class)
		if err := controllerutil.SetControllerReference(&preparation, &job, r.Scheme()); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, &job); err != nil {
			return ctrl.Result{}, err
		}
		return r.setPreparationStatus(ctx, &preparation, conductorv1alpha1.PackagePreparationPending, &job)
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if !metav1.IsControlledBy(&job, &preparation) {
		// Same handshake as ModelDownload: deterministic names never authorize
		// adopting an unrelated Job without both identity labels.
		if job.Labels[packagePreparationLabel] != preparation.Name || job.Labels[packagePreparationOwner] != string(preparation.UID) {
			return r.setPreparationFailure(ctx, &preparation, "JobOwnershipConflict", fmt.Sprintf("job %s is not owned by this PackagePreparation", job.Name))
		}
		if err := controllerutil.SetControllerReference(&preparation, &job, r.Scheme()); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Update(ctx, &job); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	return r.setPreparationStatus(ctx, &preparation, packagePreparationPhase(&job), &job)
}

func desiredPackagePreparationJob(preparation *conductorv1alpha1.PackagePreparation, class *conductorv1alpha1.PreparerClass) batchv1.Job {
	pullPolicy := class.Spec.ImagePullPolicy
	if pullPolicy == "" {
		pullPolicy = corev1.PullIfNotPresent
	}
	container := corev1.Container{
		Name:            "preparer",
		Image:           class.Spec.Image,
		ImagePullPolicy: pullPolicy,
		Args:            preparation.Spec.Args,
		Env:             BuildPreparationEnv(preparation),
		VolumeMounts:    []corev1.VolumeMount{{Name: "model-cache", MountPath: preparationMountPath}},
	}
	if preparation.Spec.Resources != nil {
		container.Resources = *preparation.Spec.Resources.DeepCopy()
	}
	return batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PackagePreparationJobName(preparation.Name),
			Namespace: preparation.Namespace,
			Labels:    map[string]string{packagePreparationLabel: preparation.Name, packagePreparationOwner: string(preparation.UID)},
		},
		Spec: batchv1.JobSpec{
			// Preparation is expensive and non-idempotent mid-run: a failed pod
			// must not be silently retried into a half-written output directory.
			BackoffLimit: pointer(int32(0)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{packagePreparationLabel: preparation.Name}},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					NodeSelector:       preparation.Spec.Cache.NodeSelector,
					ServiceAccountName: class.Spec.ServiceAccountName,
					Volumes:            []corev1.Volume{{Name: "model-cache", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: preparation.Spec.Cache.PVC}}}},
					Containers:         []corev1.Container{container},
				},
			},
		},
	}
}

func packagePreparationPhase(job *batchv1.Job) conductorv1alpha1.PackagePreparationPhase {
	if job.Status.Failed > 0 {
		return conductorv1alpha1.PackagePreparationFailed
	}
	if job.Status.Succeeded > 0 {
		return conductorv1alpha1.PackagePreparationSucceeded
	}
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return conductorv1alpha1.PackagePreparationFailed
		}
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return conductorv1alpha1.PackagePreparationSucceeded
		}
	}
	if job.Status.Active > 0 {
		return conductorv1alpha1.PackagePreparationRunning
	}
	return conductorv1alpha1.PackagePreparationPending
}

func (r *PackagePreparationReconciler) setPreparationFailure(ctx context.Context, preparation *conductorv1alpha1.PackagePreparation, reason, message string) (ctrl.Result, error) {
	return r.setPreparationStatus(ctx, preparation, conductorv1alpha1.PackagePreparationFailed, nil, reason, message)
}

func (r *PackagePreparationReconciler) setPreparationStatus(ctx context.Context, preparation *conductorv1alpha1.PackagePreparation, phase conductorv1alpha1.PackagePreparationPhase, job *batchv1.Job, failure ...string) (ctrl.Result, error) {
	previous := preparation.Status.DeepCopy()
	preparation.Status.ObservedGeneration = preparation.Generation
	preparation.Status.Phase = phase
	if job != nil {
		preparation.Status.JobName = job.Name
	}
	preparation.Status.OutputPath = preparation.Spec.OutputPath
	preparation.Status.ReceiptPath = preparation.Spec.ReceiptPath
	now := metav1.NewTime(r.Clock.Now())
	condition := metav1.Condition{Type: "Ready", ObservedGeneration: preparation.Generation, LastTransitionTime: now}
	switch phase {
	case conductorv1alpha1.PackagePreparationSucceeded:
		condition.Status, condition.Reason = metav1.ConditionTrue, "JobSucceeded"
	case conductorv1alpha1.PackagePreparationFailed:
		condition.Status, condition.Reason = metav1.ConditionFalse, "JobFailed"
	default:
		condition.Status, condition.Reason = metav1.ConditionUnknown, "JobProgressing"
	}
	if len(failure) >= 2 {
		condition.Reason, condition.Message = failure[0], failure[1]
	}
	apimeta.SetStatusCondition(&preparation.Status.Conditions, condition)
	if equality.Semantic.DeepEqual(previous, &preparation.Status) {
		if phase == conductorv1alpha1.PackagePreparationPending || phase == conductorv1alpha1.PackagePreparationRunning || phase == conductorv1alpha1.PackagePreparationWaitingForDownload {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, nil
	}
	if err := r.Status().Update(ctx, preparation); err != nil {
		return ctrl.Result{}, err
	}
	if phase == conductorv1alpha1.PackagePreparationPending || phase == conductorv1alpha1.PackagePreparationRunning || phase == conductorv1alpha1.PackagePreparationWaitingForDownload {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func (r *PackagePreparationReconciler) mapPreparerClass(ctx context.Context, obj client.Object) []reconcile.Request {
	var preparations conductorv1alpha1.PackagePreparationList
	if err := r.List(ctx, &preparations); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0)
	for i := range preparations.Items {
		if preparations.Items[i].Spec.PreparerRef == obj.GetName() {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: preparations.Items[i].Namespace, Name: preparations.Items[i].Name}})
		}
	}
	return requests
}

func (r *PackagePreparationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&conductorv1alpha1.PackagePreparation{}).
		Owns(&batchv1.Job{}).
		Watches(&conductorv1alpha1.PreparerClass{}, handler.EnqueueRequestsFromMapFunc(r.mapPreparerClass)).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}
