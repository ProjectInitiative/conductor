package controller

import (
	"context"
	"testing"
	"time"

	conductorv1alpha1 "github.com/projectinitiative/conductor/pkg/apis/conductor/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{conductorv1alpha1.AddToScheme, appsv1.AddToScheme, corev1.AddToScheme, discoveryv1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func i32(v int32) *int32 { return &v }

func workloadObjects(mode conductorv1alpha1.ManagementMode) (*conductorv1alpha1.ResourcePool, *conductorv1alpha1.ManagedWorkload, *appsv1.Deployment, *appsv1.Deployment) {
	pool := &conductorv1alpha1.ResourcePool{
		TypeMeta:   metav1.TypeMeta{APIVersion: conductorv1alpha1.GroupVersion.String(), Kind: "ResourcePool"},
		ObjectMeta: metav1.ObjectMeta{Name: "dgx", Namespace: "llm-test"},
		Spec:       conductorv1alpha1.ResourcePoolSpec{ManagementMode: mode},
	}
	workload := &conductorv1alpha1.ManagedWorkload{
		TypeMeta:   metav1.TypeMeta{APIVersion: conductorv1alpha1.GroupVersion.String(), Kind: "ManagedWorkload"},
		ObjectMeta: metav1.ObjectMeta{Name: "tensorfold", Namespace: "llm-test", UID: "uid-1", Generation: 1},
		Spec: conductorv1alpha1.ManagedWorkloadSpec{
			PoolRef: "dgx", DesiredState: conductorv1alpha1.DesiredResident,
			Driver: conductorv1alpha1.WorkloadDriver{Type: "DeploymentSet", DeploymentSet: &conductorv1alpha1.DeploymentSetSpec{RecoveryPolicy: "RecreateTogether", Members: []conductorv1alpha1.DeploymentMember{
				{Name: "worker", Role: "worker", StartOrder: 10, StopOrder: 20, Replicas: 1},
				{Name: "head", Role: "head", StartOrder: 20, StopOrder: 10, Replicas: 1},
			}}},
		},
	}
	mkDep := func(name string) *appsv1.Deployment {
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "llm-test", Generation: 1},
			Spec:       appsv1.DeploymentSpec{Replicas: i32(1), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}}}},
			Status:     appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1},
		}
	}
	return pool, workload, mkDep("worker"), mkDep("head")
}

func TestObserveModeNeverMutatesDeployments(t *testing.T) {
	pool, workload, worker, head := workloadObjects(conductorv1alpha1.ManagementObserve)
	worker.Status.AvailableReplicas = 0
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(workload).WithObjects(pool, workload, worker, head).Build()
	r := &WorkloadReconciler{Client: client, APIReader: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}}); err != nil {
		t.Fatal(err)
	}
	var got conductorv1alpha1.ManagedWorkload
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != PhaseObserving {
		t.Fatalf("phase=%q", got.Status.Phase)
	}
	var gotHead appsv1.Deployment
	_ = client.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "head"}, &gotHead)
	if *gotHead.Spec.Replicas != 1 {
		t.Fatalf("observe mode scaled head to %d", *gotHead.Spec.Replicas)
	}
}

func TestHealthyManagedWorkloadBecomesReady(t *testing.T) {
	pool, workload, worker, head := workloadObjects(conductorv1alpha1.ManagementManage)
	ready := true
	workload.Spec.Routing = &conductorv1alpha1.RoutingSpec{ServiceName: "stable"}
	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "stable-1", Namespace: "llm-test", Labels: map[string]string{discoveryv1.LabelServiceName: "stable"}}, Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"10.0.0.1"}, Conditions: discoveryv1.EndpointConditions{Ready: &ready}}}}
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(workload).WithObjects(pool, workload, worker, head, slice).Build()
	r := &WorkloadReconciler{Client: client, APIReader: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}}); err != nil {
		t.Fatal(err)
	}
	var got conductorv1alpha1.ManagedWorkload
	_ = client.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}, &got)
	if got.Status.Phase != PhaseReady || got.Status.ReadyEndpoints != 1 {
		t.Fatalf("status=%+v", got.Status)
	}
}

func TestInvalidManagementModeFailsClosed(t *testing.T) {
	pool, workload, worker, head := workloadObjects(conductorv1alpha1.ManagementMode("Mange"))
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(workload).WithObjects(pool, workload, worker, head).Build()
	r := &WorkloadReconciler{Client: client, APIReader: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}}); err != nil {
		t.Fatal(err)
	}
	var got conductorv1alpha1.ManagedWorkload
	_ = client.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}, &got)
	if got.Status.Phase != PhaseBlocked {
		t.Fatalf("invalid mode did not block: %+v", got.Status)
	}
	var gotHead appsv1.Deployment
	_ = client.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "head"}, &gotHead)
	if *gotHead.Spec.Replicas != 1 {
		t.Fatalf("invalid mode scaled head")
	}
}

func TestStartingWorkersWaitsForAlreadyScaledUnreadyWorker(t *testing.T) {
	pool, workload, worker, head := workloadObjects(conductorv1alpha1.ManagementManage)
	worker.Status.AvailableReplicas = 0
	worker.Status.ReadyReplicas = 0
	workload.Status.Phase = PhaseStartingWorkers
	workload.Status.Operation = &conductorv1alpha1.OperationStatus{Phase: PhaseStartingWorkers}
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(workload).WithObjects(pool, workload, worker, head).Build()
	r := &WorkloadReconciler{Client: client, APIReader: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}}); err != nil {
		t.Fatal(err)
	}
	var got conductorv1alpha1.ManagedWorkload
	_ = client.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}, &got)
	if got.Status.Phase != PhaseWaitingWorkerReady || got.Status.Operation.CurrentMember != "worker" {
		t.Fatalf("controller skipped unready worker: %+v", got.Status)
	}
}

func TestOldFailedPodDoesNotPoisonAvailableGeneration(t *testing.T) {
	_, _, worker, _ := workloadObjects(conductorv1alpha1.ManagementManage)
	old := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-old"}, Status: corev1.PodStatus{Phase: corev1.PodFailed}}
	healthy, reason := assess([]observedMember{{Spec: conductorv1alpha1.DeploymentMember{Name: "worker", Replicas: 1}, Deployment: *worker, Pods: []corev1.Pod{old}}}, conductorv1alpha1.DesiredResident)
	if !healthy {
		t.Fatalf("old failed pod poisoned healthy generation: %s", reason)
	}
}

func TestOverridePrecedenceAndExpiry(t *testing.T) {
	_, workload, _, _ := workloadObjects(conductorv1alpha1.ManagementObserve)
	now := time.Unix(100, 0)
	expired := metav1.NewTime(now.Add(-time.Minute))
	active := metav1.NewTime(now.Add(time.Hour))
	overrides := []runtime.Object{
		&conductorv1alpha1.WorkloadOverride{ObjectMeta: metav1.ObjectMeta{Name: "old", Namespace: "llm-test", CreationTimestamp: metav1.NewTime(now.Add(-time.Hour))}, Spec: conductorv1alpha1.WorkloadOverrideSpec{PoolRef: "dgx", WorkloadRef: "tensorfold", Mode: "Suspend", ExpiresAt: &expired}},
		&conductorv1alpha1.WorkloadOverride{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "llm-test", CreationTimestamp: metav1.NewTime(now)}, Spec: conductorv1alpha1.WorkloadOverrideSpec{PoolRef: "dgx", WorkloadRef: "tensorfold", Mode: "Suspend", ExpiresAt: &active}},
	}
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(overrides...).Build()
	r := &WorkloadReconciler{Client: client, Clock: clocktesting.NewFakeClock(now)}
	desired, source, err := r.effectiveIntent(context.Background(), workload)
	if err != nil {
		t.Fatal(err)
	}
	if desired != conductorv1alpha1.DesiredStandby || source != "Override/web" {
		t.Fatalf("desired=%s source=%s", desired, source)
	}
}
