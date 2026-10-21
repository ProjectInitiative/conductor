package controller

import (
	"context"
	"testing"
	"time"

	conductorv1alpha1 "github.com/projectinitiative/conductor/pkg/apis/conductor/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func modelDownloadScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{conductorv1alpha1.AddToScheme, batchv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	return scheme
}

func envValue(env []corev1.EnvVar, name string) string {
	for _, value := range env {
		if value.Name == name {
			return value.Value
		}
	}
	return ""
}

func TestBuildDownloaderEnvEncodesOperationAndModels(t *testing.T) {
	download := &conductorv1alpha1.ModelDownload{Spec: conductorv1alpha1.ModelDownloadSpec{
		Operation:               conductorv1alpha1.ModelDownloadEnsure,
		Models:                  []conductorv1alpha1.PinnedModel{{Repository: "org/model", Revision: "sha256:abc", Include: []string{"*.safetensors"}}},
		RateLimit:               conductorv1alpha1.ModelDownloadRateLimit{BytesPerSecond: 1234},
		Workers:                 conductorv1alpha1.ModelDownloadWorkers{FileWorkers: 2, RepositoryWorkers: 3, DeleteWorkers: 4},
		ProgressIntervalSeconds: 5,
	}}
	env, err := BuildDownloaderEnv(download, &conductorv1alpha1.DownloaderClass{})
	if err != nil {
		t.Fatal(err)
	}
	if got := envValue(env, "PULL_MODELS"); got != "org/model@sha256:abc#*.safetensors" {
		t.Fatalf("unexpected PULL_MODELS: %q", got)
	}
	if got := envValue(env, "DELETE_MODELS"); got != "" {
		t.Fatalf("DELETE_MODELS = %q", got)
	}
	if got := envValue(env, "RATE"); got != "1234" {
		t.Fatalf("RATE = %q", got)
	}
	if got := envValue(env, "MAX_FILE_WORKERS"); got != "2" {
		t.Fatalf("MAX_FILE_WORKERS = %q", got)
	}
	if got := envValue(env, "PROGRESS_INTERVAL"); got != "5" {
		t.Fatalf("PROGRESS_INTERVAL = %q", got)
	}
}

func TestDesiredModelDownloadJob(t *testing.T) {
	download := &conductorv1alpha1.ModelDownload{ObjectMeta: metav1.ObjectMeta{Name: "download", Namespace: "models"}, Spec: conductorv1alpha1.ModelDownloadSpec{Cache: conductorv1alpha1.ModelDownloadCache{PVC: "shared-cache", NodeSelector: map[string]string{"disk": "ssd"}}}}
	job := desiredModelDownloadJob(download, &conductorv1alpha1.DownloaderClass{Spec: conductorv1alpha1.DownloaderClassSpec{Image: "registry/downloader:v1"}}, []corev1.EnvVar{{Name: "PULL_MODELS", Value: "[]"}})
	if job.Name != "download-download" {
		t.Fatalf("job name = %q", job.Name)
	}
	if job.Spec.Template.Spec.NodeSelector["disk"] != "ssd" {
		t.Fatalf("node selector missing: %#v", job.Spec.Template.Spec.NodeSelector)
	}
	if got := job.Spec.Template.Spec.Containers[0].Image; got != "registry/downloader:v1" {
		t.Fatalf("image = %q", got)
	}
	if got := job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName; got != "shared-cache" {
		t.Fatalf("PVC = %q", got)
	}
	if got := ModelDownloadJobName(string(make([]byte, 100))); len(got) > 63 {
		t.Fatalf("long job name has length %d", len(got))
	}
}

func TestModelDownloadReconcileStatusTransitions(t *testing.T) {
	download := &conductorv1alpha1.ModelDownload{ObjectMeta: metav1.ObjectMeta{Name: "pull", Namespace: "models", UID: "download-uid", Generation: 1}, Spec: conductorv1alpha1.ModelDownloadSpec{Operation: conductorv1alpha1.ModelDownloadEnsure, DownloaderRef: "default", Cache: conductorv1alpha1.ModelDownloadCache{PVC: "cache"}, Models: []conductorv1alpha1.PinnedModel{{Repository: "repo", Revision: "rev"}}}}
	class := &conductorv1alpha1.DownloaderClass{ObjectMeta: metav1.ObjectMeta{Name: "default"}, Spec: conductorv1alpha1.DownloaderClassSpec{Image: "downloader:v1"}}
	scheme := modelDownloadScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(download).WithObjects(download, class).Build()
	r := &ModelDownloadReconciler{Client: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "models", Name: "pull"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	var job batchv1.Job
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "models", Name: "pull-download"}, &job); err != nil {
		t.Fatal(err)
	}
	var got conductorv1alpha1.ModelDownload
	if err := client.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != conductorv1alpha1.ModelDownloadPending || got.Status.JobName != job.Name {
		t.Fatalf("created status = %#v", got.Status)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if err := client.Status().Update(context.Background(), &job); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), req.NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != conductorv1alpha1.ModelDownloadSucceeded {
		t.Fatalf("completed status = %#v", got.Status)
	}
}
