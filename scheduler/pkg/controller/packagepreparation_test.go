package controller

import (
	"context"
	"testing"
	"time"

	conductorv1alpha1 "github.com/projectinitiative/conductor/pkg/apis/conductor/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func packagePreparationScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{conductorv1alpha1.AddToScheme, batchv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	return scheme
}

func TestBuildPreparationEnvInjectsStablePaths(t *testing.T) {
	preparation := &conductorv1alpha1.PackagePreparation{Spec: conductorv1alpha1.PackagePreparationSpec{
		OutputPath:  "packages/glm53-tp2",
		ReceiptPath: "packages/glm53-tp2/receipt.json",
	}}
	env := BuildPreparationEnv(preparation)
	if got := envValue(env, "INPUT_PATH"); got != "/models/raw" {
		t.Fatalf("INPUT_PATH = %q", got)
	}
	if got := envValue(env, "OUTPUT_PATH"); got != "/models/packages/glm53-tp2" {
		t.Fatalf("OUTPUT_PATH = %q", got)
	}
	if got := envValue(env, "RECEIPT_PATH"); got != "/models/packages/glm53-tp2/receipt.json" {
		t.Fatalf("RECEIPT_PATH = %q", got)
	}
}

func TestDesiredPackagePreparationJob(t *testing.T) {
	preparation := &conductorv1alpha1.PackagePreparation{
		ObjectMeta: metav1.ObjectMeta{Name: "glm53-tp2", Namespace: "models"},
		Spec: conductorv1alpha1.PackagePreparationSpec{
			Cache: conductorv1alpha1.ModelDownloadCache{
				PVC:          "shared-cache",
				NodeSelector: map[string]string{"ai-compute": "true"},
			},
			Args: []string{"export", "--ranks", "2"},
			Resources: &corev1.ResourceRequirements{
				Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")},
			},
		},
	}
	job := desiredPackagePreparationJob(preparation, &conductorv1alpha1.PreparerClass{Spec: conductorv1alpha1.PreparerClassSpec{Image: "registry/prep:v1"}})
	if job.Name != "glm53-tp2-prepare" {
		t.Fatalf("job name = %q", job.Name)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if container.Image != "registry/prep:v1" {
		t.Fatalf("image = %q", container.Image)
	}
	if len(container.Args) != 3 || container.Args[0] != "export" {
		t.Fatalf("args = %#v", container.Args)
	}
	if gpu, ok := container.Resources.Limits["nvidia.com/gpu"]; !ok || gpu.String() != "1" {
		t.Fatalf("gpu limit missing: %#v", container.Resources.Limits)
	}
	if job.Spec.Template.Spec.NodeSelector["ai-compute"] != "true" {
		t.Fatalf("node selector missing")
	}
	if got := job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName; got != "shared-cache" {
		t.Fatalf("pvc = %q", got)
	}
	if *job.Spec.BackoffLimit != 0 {
		t.Fatalf("backoff limit must be zero to avoid partial output on retry")
	}
}

func TestPackagePreparationWaitsForModelDownloadGate(t *testing.T) {
	scheme := packagePreparationScheme(t)
	fakeClock := clocktesting.NewFakeClock(time.Now())
	preparation := &conductorv1alpha1.PackagePreparation{
		ObjectMeta: metav1.ObjectMeta{Name: "glm53-tp2", Namespace: "models", UID: "prep-uid"},
		Spec: conductorv1alpha1.PackagePreparationSpec{
			PreparerRef:      "flare-ignition",
			ModelDownloadRef: "glm53-raw",
			Cache:            conductorv1alpha1.ModelDownloadCache{PVC: "shared-cache"},
			Args:             []string{"export"},
			OutputPath:       "packages/glm53-tp2",
			ReceiptPath:      "packages/glm53-tp2/receipt.json",
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(preparation).WithObjects(preparation).Build()
	reconciler := &PackagePreparationReconciler{Client: client, Clock: fakeClock}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "models", Name: "glm53-tp2"}})
	if err != nil {
		t.Fatal(err)
	}
	var updated conductorv1alpha1.PackagePreparation
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "models", Name: "glm53-tp2"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != conductorv1alpha1.PackagePreparationWaitingForDownload {
		t.Fatalf("phase = %q, want WaitingForDownload", updated.Status.Phase)
	}
	var jobs batchv1.JobList
	if err := client.List(context.Background(), &jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 0 {
		t.Fatalf("no Job should exist before the ModelDownload gate passes, got %d", len(jobs.Items))
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("expected periodic requeue while waiting")
	}
}

func TestPackagePreparationCreatesJobAfterGatePasses(t *testing.T) {
	scheme := packagePreparationScheme(t)
	fakeClock := clocktesting.NewFakeClock(time.Now())
	gate := &conductorv1alpha1.ModelDownload{
		ObjectMeta: metav1.ObjectMeta{Name: "glm53-raw", Namespace: "models"},
		Status:     conductorv1alpha1.ModelDownloadStatus{Phase: conductorv1alpha1.ModelDownloadSucceeded},
	}
	preparation := &conductorv1alpha1.PackagePreparation{
		ObjectMeta: metav1.ObjectMeta{Name: "glm53-tp2", Namespace: "models", UID: "prep-uid"},
		Spec: conductorv1alpha1.PackagePreparationSpec{
			PreparerRef:      "flare-ignition",
			ModelDownloadRef: "glm53-raw",
			Cache:            conductorv1alpha1.ModelDownloadCache{PVC: "shared-cache"},
			Args:             []string{"export"},
			OutputPath:       "packages/glm53-tp2",
			ReceiptPath:      "packages/glm53-tp2/receipt.json",
		},
	}
	preparerClass := &conductorv1alpha1.PreparerClass{ObjectMeta: metav1.ObjectMeta{Name: "flare-ignition"}, Spec: conductorv1alpha1.PreparerClassSpec{Image: "registry/prep:v1"}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(preparation).WithObjects(gate, preparation, preparerClass).Build()
	reconciler := &PackagePreparationReconciler{Client: client, Clock: fakeClock}

	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "models", Name: "glm53-tp2"}}); err != nil {
		t.Fatal(err)
	}
	var jobs batchv1.JobList
	if err := client.List(context.Background(), &jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 1 {
		t.Fatalf("exactly one Job expected, got %d", len(jobs.Items))
	}
	job := jobs.Items[0]
	if !metav1.IsControlledBy(&job, preparation) {
		t.Fatalf("Job must be owned by the PackagePreparation")
	}
	if envValue(job.Spec.Template.Spec.Containers[0].Env, "OUTPUT_PATH") != "/models/packages/glm53-tp2" {
		t.Fatalf("OUTPUT_PATH env missing on created Job")
	}
	var updated conductorv1alpha1.PackagePreparation
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "models", Name: "glm53-tp2"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.JobName != job.Name {
		t.Fatalf("status job name = %q", updated.Status.JobName)
	}
}

func TestPackagePreparationRejectsUnrelatedJob(t *testing.T) {
	scheme := packagePreparationScheme(t)
	fakeClock := clocktesting.NewFakeClock(time.Now())
	preparation := &conductorv1alpha1.PackagePreparation{
		ObjectMeta: metav1.ObjectMeta{Name: "glm53-tp2", Namespace: "models", UID: "prep-uid"},
		Spec: conductorv1alpha1.PackagePreparationSpec{
			PreparerRef: "flare-ignition",
			Cache:       conductorv1alpha1.ModelDownloadCache{PVC: "shared-cache"},
			Args:        []string{"export"},
			OutputPath:  "packages/glm53-tp2",
			ReceiptPath: "packages/glm53-tp2/receipt.json",
		},
	}
	impostor := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "glm53-tp2-prepare", Namespace: "models"}}
	preparerClass := &conductorv1alpha1.PreparerClass{ObjectMeta: metav1.ObjectMeta{Name: "flare-ignition"}, Spec: conductorv1alpha1.PreparerClassSpec{Image: "registry/prep:v1"}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(preparation).WithObjects(preparation, impostor, preparerClass).Build()
	reconciler := &PackagePreparationReconciler{Client: client, Clock: fakeClock}

	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "models", Name: "glm53-tp2"}}); err != nil {
		t.Fatal(err)
	}
	var updated conductorv1alpha1.PackagePreparation
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "models", Name: "glm53-tp2"}, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.Status.Phase != conductorv1alpha1.PackagePreparationFailed {
		t.Fatalf("phase = %q, want Failed", updated.Status.Phase)
	}
	reason := ""
	for _, condition := range updated.Status.Conditions {
		if condition.Type == "Ready" {
			reason = condition.Reason
		}
	}
	if reason != "JobOwnershipConflict" {
		t.Fatalf("condition reason = %q, want JobOwnershipConflict", reason)
	}
}
