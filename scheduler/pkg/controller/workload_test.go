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
	"sigs.k8s.io/controller-runtime/pkg/client"
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

type racingStatusClient struct {
	client.Client
	raced bool
}

func (c *racingStatusClient) Status() client.SubResourceWriter {
	return &racingStatusWriter{SubResourceWriter: c.Client.Status(), owner: c}
}

type racingStatusWriter struct {
	client.SubResourceWriter
	owner *racingStatusClient
}

func (w *racingStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if !w.owner.raced {
		w.owner.raced = true
		var latest conductorv1alpha1.ManagedWorkload
		if err := w.owner.Client.Get(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}, &latest); err != nil {
			return err
		}
		if latest.Annotations == nil {
			latest.Annotations = map[string]string{}
		}
		latest.Annotations["test.concurrent-update"] = "true"
		if err := w.owner.Client.Update(ctx, &latest); err != nil {
			return err
		}
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

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

func TestUpdateStatusRetriesConcurrentObjectModification(t *testing.T) {
	_, workload, _, _ := workloadObjects(conductorv1alpha1.ManagementManage)
	scheme := testScheme(t)
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(workload).WithObjects(workload).Build()
	racing := &racingStatusClient{Client: base}
	r := &WorkloadReconciler{Client: racing, APIReader: racing}
	result, err := r.updateStatus(context.Background(), workload, PhaseStartingHead, ctrl.Result{Requeue: true})
	if err != nil {
		t.Fatalf("updateStatus failed to retry conflict: %v", err)
	}
	if !result.Requeue || !racing.raced {
		t.Fatalf("result=%+v conflictInjected=%v", result, racing.raced)
	}
	var got conductorv1alpha1.ManagedWorkload
	if err := base.Get(context.Background(), types.NamespacedName{Namespace: workload.Namespace, Name: workload.Name}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != PhaseStartingHead {
		t.Fatalf("phase=%q, want %q", got.Status.Phase, PhaseStartingHead)
	}
	if got.Annotations["test.concurrent-update"] != "true" {
		t.Fatalf("concurrent metadata update was lost: %#v", got.Annotations)
	}
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

func TestContainersRunningGateAdvancesBeforeDeploymentAvailable(t *testing.T) {
	pool, workload, worker, head := workloadObjects(conductorv1alpha1.ManagementManage)
	worker.Status.AvailableReplicas = 0
	worker.Status.ReadyReplicas = 0
	workload.Spec.Driver.DeploymentSet.Members[0].ReadyGate = "ContainersRunning"
	workload.Status.Phase = PhaseStartingWorkers
	workload.Status.Operation = &conductorv1alpha1.OperationStatus{Phase: PhaseStartingWorkers}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-starting", Namespace: "llm-test", Labels: map[string]string{"app": "worker"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "rank"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "rank", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}},
	}
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(workload).WithObjects(pool, workload, worker, head, pod).Build()
	r := &WorkloadReconciler{Client: client, APIReader: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}}); err != nil {
		t.Fatal(err)
	}
	var got conductorv1alpha1.ManagedWorkload
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != PhaseStartingHead {
		t.Fatalf("ContainersRunning gate did not release head startup: phase=%q", got.Status.Phase)
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

func TestColdStartDoesNotTriggerRecovery(t *testing.T) {
	pool, workload, worker, head := workloadObjects(conductorv1alpha1.ManagementManage)
	now := time.Unix(100, 0)
	workload.Spec.Driver.DeploymentSet.StartupGracePeriod = metav1.Duration{Duration: 10 * time.Minute}
	head.Status.AvailableReplicas = 0
	head.Status.ReadyReplicas = 0
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "head-starting", Namespace: "llm-test", CreationTimestamp: metav1.NewTime(now.Add(-time.Minute)), Labels: map[string]string{"app": "head"}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, StartTime: ptrTime(now.Add(-time.Minute))}}
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(workload).WithObjects(pool, workload, worker, head, &pod).Build()
	r := &WorkloadReconciler{Client: client, APIReader: client, Clock: clocktesting.NewFakeClock(now)}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}}); err != nil {
		t.Fatal(err)
	}
	var got conductorv1alpha1.ManagedWorkload
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != PhaseObserving || got.Status.Conditions[0].Reason != "StartupInProgress" {
		t.Fatalf("cold start triggered recovery: %+v", got.Status)
	}
	var gotHead appsv1.Deployment
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "head"}, &gotHead); err != nil {
		t.Fatal(err)
	}
	if *gotHead.Spec.Replicas != 1 {
		t.Fatalf("cold start scaled head to %d", *gotHead.Spec.Replicas)
	}
}

func ptrTime(t time.Time) *metav1.Time {
	v := metav1.NewTime(t)
	return &v
}

func TestPrioritySuppressesLowerPriorityResident(t *testing.T) {
	pool, workload, _, _ := workloadObjects(conductorv1alpha1.ManagementObserve)
	workload.Spec.Priority = 50
	urgent := workload.DeepCopy()
	urgent.Name = "urgent"
	urgent.UID = "uid-urgent"
	urgent.Spec.Priority = 0
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(pool, workload, urgent).Build()
	r := &WorkloadReconciler{Client: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	desired, source, err := r.effectiveIntent(context.Background(), workload)
	if err != nil {
		t.Fatal(err)
	}
	if desired != conductorv1alpha1.DesiredStandby || source != "Priority/urgent" {
		t.Fatalf("desired=%s source=%s", desired, source)
	}
}

type testFencer struct{ result FenceResult }

func (f testFencer) EnsureFenced(context.Context, string) (FenceResult, error) { return f.result, nil }

func TestHigherPriorityWorkloadWaitsForLowerPriorityRelease(t *testing.T) {
	pool, lower, worker, head := workloadObjects(conductorv1alpha1.ManagementObserve)
	lower.Spec.Priority = 50
	higher := lower.DeepCopy()
	higher.Name = "urgent"
	higher.UID = "uid-urgent"
	higher.Spec.Priority = 0
	higher.Spec.Driver.DeploymentSet.Members = []conductorv1alpha1.DeploymentMember{{Name: "urgent-worker", Role: "worker", Replicas: 1}}
	urgentWorker := worker.DeepCopy()
	urgentWorker.Name = "urgent-worker"
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, lower, higher, worker, head, urgentWorker).Build()
	r := &WorkloadReconciler{Client: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	pending, err := r.priorityPreemptionPending(context.Background(), higher)
	if err != nil || pending != "tensorfold" {
		t.Fatalf("pending=%q err=%v", pending, err)
	}
	worker.Spec.Replicas = i32(0)
	head.Spec.Replicas = i32(0)
	if err := client.Update(context.Background(), worker); err != nil {
		t.Fatal(err)
	}
	if err := client.Update(context.Background(), head); err != nil {
		t.Fatal(err)
	}
	pending, err = r.priorityPreemptionPending(context.Background(), higher)
	if err != nil || pending != "" {
		t.Fatalf("workload did not release capacity: pending=%q err=%v", pending, err)
	}
}

func TestFencingRequiresVerifiedUnexpiredReceipt(t *testing.T) {
	now := time.Unix(100, 0)
	pool := &conductorv1alpha1.ResourcePool{Spec: conductorv1alpha1.ResourcePoolSpec{Policy: conductorv1alpha1.PoolPolicy{}}}
	base := &WorkloadReconciler{Clock: clocktesting.NewFakeClock(now)}
	blocked, _, err := base.fenceOrBlock(context.Background(), pool, "octant")
	if err != nil || !blocked {
		t.Fatalf("missing fencer was not blocked: blocked=%v err=%v", blocked, err)
	}
	base.Fencer = testFencer{result: FenceResult{NodeName: "octant", Verified: true, Evidence: "bmc-123", ExpiresAt: now.Add(time.Hour)}}
	blocked, reason, err := base.fenceOrBlock(context.Background(), pool, "octant")
	if err != nil || blocked || reason != "fenced:bmc-123" {
		t.Fatalf("valid fencing receipt was rejected: blocked=%v reason=%q err=%v", blocked, reason, err)
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

func TestWaitingWorkerReadyAbortsWhenMemberVanishes(t *testing.T) {
	pool, workload, _, head := workloadObjects(conductorv1alpha1.ManagementManage)
	// Worker Deployment deleted mid-recovery while the operation waits on it.
	workload.Status.Phase = PhaseWaitingWorkerReady
	workload.Status.Operation = &conductorv1alpha1.OperationStatus{Phase: PhaseWaitingWorkerReady, CurrentMember: "worker", UpdatedAt: metav1.NewTime(time.Unix(100, 0))}
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(workload).WithObjects(pool, workload, head).Build()
	r := &WorkloadReconciler{Client: client, APIReader: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}}); err != nil {
		t.Fatal(err)
	}
	var got conductorv1alpha1.ManagedWorkload
	_ = client.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}, &got)
	if got.Status.Phase != PhaseObserving || got.Status.Operation != nil {
		t.Fatalf("stuck operation did not abort on missing member: %+v", got.Status)
	}
}

func TestQualifyingReassertsReplicasDriftedByExternalApply(t *testing.T) {
	pool, workload, worker, head := workloadObjects(conductorv1alpha1.ManagementManage)
	// Both members available, but an external SSA apply zeroed the head
	// mid-qualification (2026-11-18 glm53-flash-1104 deadlock).
	head.Spec.Replicas = i32(0)
	workload.Status.Phase = PhaseQualifying
	workload.Status.Operation = &conductorv1alpha1.OperationStatus{Phase: PhaseQualifying, CurrentMember: "head", UpdatedAt: metav1.NewTime(time.Unix(100, 0))}
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(workload).WithObjects(pool, workload, worker, head).Build()
	r := &WorkloadReconciler{Client: client, APIReader: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}}); err != nil {
		t.Fatal(err)
	}
	var gotHead appsv1.Deployment
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "head"}, &gotHead); err != nil {
		t.Fatal(err)
	}
	if *gotHead.Spec.Replicas != 1 {
		t.Fatalf("Qualifying did not re-assert drifted replicas: head=%d", *gotHead.Spec.Replicas)
	}
}

func TestResidentWithMissingMemberHoldsInsteadOfRecoveryChurn(t *testing.T) {
	pool, workload, worker, _ := workloadObjects(conductorv1alpha1.ManagementManage)
	// Head Deployment does not exist (deleted by an Application cascade).
	// Recovery must not start: StoppingHead would churn HeadMissing forever.
	workload.Status.Phase = PhaseObserving
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(workload).WithObjects(pool, workload, worker).Build()
	r := &WorkloadReconciler{Client: client, APIReader: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}}); err != nil {
		t.Fatal(err)
	}
	var got conductorv1alpha1.ManagedWorkload
	_ = client.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}, &got)
	if got.Status.Phase != PhaseObserving || got.Status.Operation != nil {
		t.Fatalf("missing member triggered recovery churn: %+v", got.Status)
	}
	progressing := false
	for _, c := range got.Status.Conditions {
		if c.Type == ConditionProgressing && c.Reason == "WaitingForMember" {
			progressing = true
		}
	}
	if !progressing {
		t.Fatalf("expected WaitingForMember progressing condition: %+v", got.Status.Conditions)
	}
}

func TestStaleOperationIsWatchdogRestarted(t *testing.T) {
	pool, workload, worker, head := workloadObjects(conductorv1alpha1.ManagementManage)
	// Operation frozen in a wait phase far beyond the startup grace period.
	workload.Spec.Driver.DeploymentSet.StartupGracePeriod = metav1.Duration{Duration: 30 * time.Minute}
	workload.Status.Phase = PhaseQualifying
	workload.Status.Operation = &conductorv1alpha1.OperationStatus{Phase: PhaseQualifying, CurrentMember: "head", UpdatedAt: metav1.NewTime(time.Unix(0, 0))}
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(workload).WithObjects(pool, workload, worker, head).Build()
	r := &WorkloadReconciler{Client: client, APIReader: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}}); err != nil {
		t.Fatal(err)
	}
	var got conductorv1alpha1.ManagedWorkload
	_ = client.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}, &got)
	if got.Status.Operation != nil {
		t.Fatalf("watchdog did not clear stale operation: %+v", got.Status.Operation)
	}
	// After the abort the workload re-enters from Observing; members are
	// healthy here, so the next assessment path must be a clean one (not a
	// frozen wait).
	if got.Status.Phase == PhaseQualifying {
		t.Fatalf("watchdog left workload frozen in %q", got.Status.Phase)
	}
}

func mkPoolSlots(mode conductorv1alpha1.ManagementMode, slots int32) *conductorv1alpha1.ResourcePool {
	pool := &conductorv1alpha1.ResourcePool{
		TypeMeta:   metav1.TypeMeta{APIVersion: conductorv1alpha1.GroupVersion.String(), Kind: "ResourcePool"},
		ObjectMeta: metav1.ObjectMeta{Name: "dgx", Namespace: "llm-test"},
		Spec:       conductorv1alpha1.ResourcePoolSpec{ManagementMode: mode, Capacity: conductorv1alpha1.ResourcePoolCapacity{Slots: slots}},
	}
	return pool
}

func mkResident(name string, priority int32, sparks int32) *conductorv1alpha1.ManagedWorkload {
	w := &conductorv1alpha1.ManagedWorkload{
		TypeMeta:   metav1.TypeMeta{APIVersion: conductorv1alpha1.GroupVersion.String(), Kind: "ManagedWorkload"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "llm-test", UID: types.UID("uid-" + name), Generation: 1},
		Spec: conductorv1alpha1.ManagedWorkloadSpec{
			PoolRef: "dgx", DesiredState: conductorv1alpha1.DesiredResident, Priority: priority,
			Driver: conductorv1alpha1.WorkloadDriver{Type: "DeploymentSet", DeploymentSet: &conductorv1alpha1.DeploymentSetSpec{
				RecoveryPolicy: "RecreateTogether", Sparks: sparks,
				Members: []conductorv1alpha1.DeploymentMember{
					{Name: name + "-worker", Role: "worker", StartOrder: 10, StopOrder: 20, Replicas: 1},
					{Name: name + "-head", Role: "head", StartOrder: 20, StopOrder: 10, Replicas: 1},
				}}},
		},
	}
	return w
}

// Two TP2 residents fit a 4-spark pool: both admitted (the A+B co-residency case).
func TestCapacityAdmitsTwoSparkPairsOnFourSparkPool(t *testing.T) {
	pool := mkPoolSlots(conductorv1alpha1.ManagementManage, 4)
	a := mkResident("flash-a", 90, 2)
	b := mkResident("flash-b", 80, 2)
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(pool, a, b).Build()
	r := &WorkloadReconciler{Client: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	for _, w := range []*conductorv1alpha1.ManagedWorkload{a, b} {
		desired, source, err := r.effectiveIntent(context.Background(), w)
		if err != nil {
			t.Fatal(err)
		}
		if desired != conductorv1alpha1.DesiredResident || source != "Baseline" {
			t.Fatalf("%s: desired=%s source=%s", w.Name, desired, source)
		}
	}
}

// A 3-spark resident + a 2-spark resident do not both fit a 4-spark pool:
// the more urgent (lower priority value) is admitted, the other stranded.
func TestCapacityStrandsLowerPriorityWhenDemandExceeds(t *testing.T) {
	pool := mkPoolSlots(conductorv1alpha1.ManagementManage, 4)
	big := mkResident("big-tp3", 10, 3)
	small := mkResident("small-tp2", 50, 2)
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(pool, big, small).Build()
	r := &WorkloadReconciler{Client: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	desired, source, err := r.effectiveIntent(context.Background(), big)
	if err != nil || desired != conductorv1alpha1.DesiredResident {
		t.Fatalf("big: desired=%s err=%v", desired, err)
	}
	desired, source, err = r.effectiveIntent(context.Background(), small)
	if err != nil || desired != conductorv1alpha1.DesiredStandby || source != "Priority/big-tp3" {
		t.Fatalf("small: desired=%s source=%s err=%v", desired, source, err)
	}
}

// Sparks unset defaults to member replica count (a TP2 pair demands 2).
func TestSparksDefaultToMemberReplicas(t *testing.T) {
	pool := mkPoolSlots(conductorv1alpha1.ManagementManage, 3)
	a := mkResident("a", 90, 0)
	b := mkResident("b", 80, 0)
	a.Spec.Driver.DeploymentSet.Sparks = 0
	b.Spec.Driver.DeploymentSet.Sparks = 0
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(pool, a, b).Build()
	r := &WorkloadReconciler{Client: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	// 2+2 > 3: a cannot fit after b (b is more urgent at 80 vs 90).
	desired, source, err := r.effectiveIntent(context.Background(), a)
	if err != nil || desired != conductorv1alpha1.DesiredStandby || source != "Priority/b" {
		t.Fatalf("a: desired=%s source=%s err=%v", desired, source, err)
	}
	desired, _, err = r.effectiveIntent(context.Background(), b)
	if err != nil || desired != conductorv1alpha1.DesiredResident {
		t.Fatalf("b: desired=%s err=%v", desired, err)
	}
}

// The most urgent resident is admitted even when its demand exceeds capacity
// (oversized urgent request wins the pool, preserving old winner semantics).
func TestOversizedUrgentWorkloadStillAdmitted(t *testing.T) {
	pool := mkPoolSlots(conductorv1alpha1.ManagementManage, 2)
	big := mkResident("tp4", 10, 4)
	small := mkResident("tp2", 50, 2)
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(pool, big, small).Build()
	r := &WorkloadReconciler{Client: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	desired, _, err := r.effectiveIntent(context.Background(), big)
	if err != nil || desired != conductorv1alpha1.DesiredResident {
		t.Fatalf("tp4: desired=%s err=%v", desired, err)
	}
	desired, _, err = r.effectiveIntent(context.Background(), small)
	if err != nil || desired != conductorv1alpha1.DesiredStandby {
		t.Fatalf("tp2: desired=%s err=%v", desired, err)
	}
}

// An admitted resident waits for a stranded resident's deployments to drain
// before occupying (fencing handover).
func TestAdmittedResidentWaitsForStrandedDrain(t *testing.T) {
	pool := mkPoolSlots(conductorv1alpha1.ManagementManage, 4)
	big := mkResident("big-tp3", 10, 3)
	small := mkResident("small-tp2", 50, 2)
	smallWorker := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "small-tp2-worker", Namespace: "llm-test"},
		Spec: appsv1.DeploymentSpec{Replicas: i32(1), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "small-tp2-worker"}},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "small-tp2-worker"}}}}}
	smallHead := smallWorker.DeepCopy()
	smallHead.Name = "small-tp2-head"
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, big, small, smallWorker, smallHead).Build()
	r := &WorkloadReconciler{Client: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	pending, err := r.priorityPreemptionPending(context.Background(), big)
	if err != nil || pending != "small-tp2" {
		t.Fatalf("pending=%q err=%v", pending, err)
	}
	smallWorker.Spec.Replicas = i32(0)
	smallHead.Spec.Replicas = i32(0)
	if err := client.Update(context.Background(), smallWorker); err != nil {
		t.Fatal(err)
	}
	if err := client.Update(context.Background(), smallHead); err != nil {
		t.Fatal(err)
	}
	pending, err = r.priorityPreemptionPending(context.Background(), big)
	if err != nil || pending != "" {
		t.Fatalf("drain did not clear: pending=%q err=%v", pending, err)
	}
}

// Two co-residents stay admitted after one re-reconciles (plan stability).
func TestPlanStableAcrossReconciles(t *testing.T) {
	pool := mkPoolSlots(conductorv1alpha1.ManagementManage, 4)
	a := mkResident("flash-a", 90, 2)
	b := mkResident("flash-b", 80, 2)
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(pool, a, b).Build()
	r := &WorkloadReconciler{Client: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	for round := 0; round < 3; round++ {
		for _, w := range []*conductorv1alpha1.ManagedWorkload{a, b} {
			desired, _, err := r.effectiveIntent(context.Background(), w)
			if err != nil || desired != conductorv1alpha1.DesiredResident {
				t.Fatalf("round %d %s: desired=%s err=%v", round, w.Name, desired, err)
			}
		}
	}
}

// Two admitted residents co-exist: neither waits on the other (the
// co-residency deadlock regression — an admitted workload must only wait
// for STRANDED residents to drain, never for fellow admitted ones).
func TestAdmittedResidentsDoNotWaitOnEachOther(t *testing.T) {
	pool := mkPoolSlots(conductorv1alpha1.ManagementManage, 4)
	a := mkResident("flash-a", 90, 2)
	b := mkResident("flash-b", 80, 2)
	aWorker := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "flash-a-worker", Namespace: "llm-test"},
		Spec: appsv1.DeploymentSpec{Replicas: i32(1), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "flash-a-worker"}},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "flash-a-worker"}}}}}
	aHead := aWorker.DeepCopy()
	aHead.Name = "flash-a-head"
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, a, b, aWorker, aHead).Build()
	r := &WorkloadReconciler{Client: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0))}
	pending, err := r.priorityPreemptionPending(context.Background(), b)
	if err != nil || pending != "" {
		t.Fatalf("b waits on admitted a: pending=%q err=%v", pending, err)
	}
}
