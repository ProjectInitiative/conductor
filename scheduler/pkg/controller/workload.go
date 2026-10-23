// Package controller implements Conductor's Kubernetes-native reconciliation
// loop. It intentionally manages only resources named by ManagedWorkload CRs;
// name-based discovery remains a break-glass CLI concern.
package controller

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	conductorv1alpha1 "github.com/projectinitiative/conductor/pkg/apis/conductor/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	PhaseObserving          = "Observing"
	PhaseReady              = "Ready"
	PhaseBlocked            = "Blocked"
	PhaseStoppingHead       = "StoppingHead"
	PhaseWaitingHeadGone    = "WaitingHeadGone"
	PhaseStoppingWorkers    = "StoppingWorkers"
	PhaseWaitingAllGone     = "WaitingAllGone"
	PhaseStartingWorkers    = "StartingWorkers"
	PhaseWaitingWorkerReady = "WaitingWorkerReady"
	PhaseStartingHead       = "StartingHead"
	PhaseQualifying         = "Qualifying"

	defaultStartupGracePeriod = 30 * time.Minute

	ConditionReady       = "Ready"
	ConditionProgressing = "Progressing"
	ConditionDegraded    = "Degraded"
	ConditionBlocked     = "Blocked"
)

// WorkloadReconciler reconciles one explicit ManagedWorkload at a time. Each
// pass performs at most one bounded mutation and persists the next phase, so a
// controller restart or leader change safely resumes the operation.
type WorkloadReconciler struct {
	client.Client
	APIReader     client.Reader
	Clock         clock.Clock
	Recorder      record.EventRecorder
	HealthChecker HealthChecker
	Fencer        Fencer
}

// HealthChecker makes direct service qualification injectable in unit tests.
type HealthChecker interface {
	Check(context.Context, string) error
}

// FenceResult is evidence returned by an idempotent fencing provider. A
// provider must not report Verified until it has independently established
// that the node cannot continue running workload processes.
type FenceResult struct {
	NodeName  string
	Verified  bool
	Evidence  string
	ExpiresAt time.Time
}

// Fencer is deliberately injected rather than implemented with kubectl or a
// cloud-specific API. EnsureFenced must be idempotent and independently verify
// power isolation, so a transient NotReady observation cannot cause destructive
// cleanup or split-brain relocation.
type Fencer interface {
	EnsureFenced(context.Context, string) (FenceResult, error)
}

type HTTPHealthChecker struct{ Client *http.Client }

func (h HTTPHealthChecker) Check(ctx context.Context, url string) error {
	client := h.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("health returned HTTP %d", resp.StatusCode)
	}
	return nil
}

// Reconcile implements the bounded DeploymentSet state machine.
func (r *WorkloadReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var workload conductorv1alpha1.ManagedWorkload
	if err := r.Get(ctx, req.NamespacedName, &workload); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if r.Clock == nil {
		r.Clock = clock.RealClock{}
	}

	var pool conductorv1alpha1.ResourcePool
	if err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: workload.Spec.PoolRef}, &pool); err != nil {
		return r.failStatus(ctx, &workload, PhaseBlocked, "PoolNotFound", err.Error(), true)
	}
	if workload.Spec.Driver.Type != "DeploymentSet" || workload.Spec.Driver.DeploymentSet == nil {
		return r.failStatus(ctx, &workload, PhaseBlocked, "UnsupportedDriver", "only DeploymentSet is implemented", true)
	}
	members, err := r.observeMembers(ctx, &workload)
	if err != nil {
		return ctrl.Result{}, err
	}
	workload.Status.Members = memberStatuses(members)
	workload.Status.ObservedGeneration = workload.Generation

	desired, source, err := r.effectiveIntent(ctx, &workload)
	if err != nil {
		return ctrl.Result{}, err
	}
	workload.Status.EffectiveIntent = string(desired)
	workload.Status.IntentSource = source

	mode := pool.Spec.ManagementMode
	if mode == "" {
		mode = conductorv1alpha1.ManagementObserve
	}
	if mode != conductorv1alpha1.ManagementObserve && mode != conductorv1alpha1.ManagementManage {
		return r.failStatus(ctx, &workload, PhaseBlocked, "InvalidManagementMode", fmt.Sprintf("unsupported managementMode %q", mode), true)
	}
	if mode == conductorv1alpha1.ManagementObserve || workload.Spec.Paused {
		healthy, reason := assess(members, desired)
		setCondition(&workload.Status.Conditions, metav1.Condition{Type: ConditionReady, Status: boolCondition(healthy), Reason: reason, ObservedGeneration: workload.Generation})
		return r.updateStatus(ctx, &workload, PhaseObserving, ctrl.Result{RequeueAfter: 30 * time.Second})
	}

	blocked, reason, err := r.nodeSafety(ctx, &pool, members)
	if err != nil {
		return ctrl.Result{}, err
	}
	if blocked {
		return r.failStatus(ctx, &workload, PhaseBlocked, "NodeNeedsFencing", reason, true)
	}

	if desired != conductorv1alpha1.DesiredResident {
		return r.reconcileStandby(ctx, &workload, members)
	}
	return r.reconcileResident(ctx, &workload, members)
}

type observedMember struct {
	Spec       conductorv1alpha1.DeploymentMember
	Deployment appsv1.Deployment
	Pods       []corev1.Pod
}

func (r *WorkloadReconciler) observeMembers(ctx context.Context, workload *conductorv1alpha1.ManagedWorkload) ([]observedMember, error) {
	out := make([]observedMember, 0, len(workload.Spec.Driver.DeploymentSet.Members))
	for _, member := range workload.Spec.Driver.DeploymentSet.Members {
		var dep appsv1.Deployment
		if err := r.Get(ctx, types.NamespacedName{Namespace: workload.Namespace, Name: member.Name}, &dep); err != nil {
			if apierrors.IsNotFound(err) {
				out = append(out, observedMember{Spec: member})
				continue
			}
			return nil, err
		}
		var pods corev1.PodList
		selector, err := metav1.LabelSelectorAsSelector(dep.Spec.Selector)
		if err != nil {
			return nil, fmt.Errorf("deployment %s selector: %w", dep.Name, err)
		}
		if err := r.List(ctx, &pods, client.InNamespace(workload.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
			return nil, err
		}
		out = append(out, observedMember{Spec: member, Deployment: dep, Pods: pods.Items})
	}
	return out, nil
}

func memberStatuses(members []observedMember) []conductorv1alpha1.MemberStatus {
	out := make([]conductorv1alpha1.MemberStatus, 0, len(members))
	for _, m := range members {
		node := ""
		for _, p := range m.Pods {
			if p.DeletionTimestamp == nil && p.Spec.NodeName != "" {
				node = p.Spec.NodeName
				break
			}
		}
		out = append(out, conductorv1alpha1.MemberStatus{Name: m.Spec.Name, Node: node, Replicas: ptrValue(m.Deployment.Spec.Replicas), Ready: m.Deployment.Status.ReadyReplicas, Available: m.Deployment.Status.AvailableReplicas, Generation: m.Deployment.Generation})
	}
	return out
}

func (r *WorkloadReconciler) effectiveIntent(ctx context.Context, workload *conductorv1alpha1.ManagedWorkload) (conductorv1alpha1.DesiredState, string, error) {
	desired := workload.Spec.DesiredState
	if desired == "" {
		desired = conductorv1alpha1.DesiredStandby
	}
	var overrides conductorv1alpha1.WorkloadOverrideList
	if err := r.List(ctx, &overrides, client.InNamespace(workload.Namespace)); err != nil {
		return desired, "Baseline", err
	}
	now := r.Clock.Now()
	sort.SliceStable(overrides.Items, func(i, j int) bool {
		return overrides.Items[i].CreationTimestamp.After(overrides.Items[j].CreationTimestamp.Time)
	})
	for _, override := range overrides.Items {
		if override.Spec.PoolRef != workload.Spec.PoolRef || (override.Spec.ExpiresAt != nil && !override.Spec.ExpiresAt.After(now)) {
			continue
		}
		switch override.Spec.Mode {
		case "Pin":
			if override.Spec.WorkloadRef == workload.Name {
				return conductorv1alpha1.DesiredResident, "Override/" + override.Name, nil
			}
			return conductorv1alpha1.DesiredStandby, "Override/" + override.Name, nil
		case "Suspend":
			if override.Spec.WorkloadRef == "" || override.Spec.WorkloadRef == workload.Name {
				return conductorv1alpha1.DesiredStandby, "Override/" + override.Name, nil
			}
		case "FreezePool":
			if workload.Status.Phase == PhaseReady {
				return conductorv1alpha1.DesiredResident, "Override/" + override.Name, nil
			}
			return conductorv1alpha1.DesiredStandby, "Override/" + override.Name, nil
		case "Automatic":
			return desired, "Override/" + override.Name, nil
		}
	}
	if desired != conductorv1alpha1.DesiredResident {
		return desired, "Baseline", nil
	}
	winner, err := r.priorityWinner(ctx, workload.Namespace, workload.Spec.PoolRef, overrides, now)
	if err != nil {
		return desired, "Baseline", err
	}
	if winner != "" && winner != workload.Name {
		return conductorv1alpha1.DesiredStandby, "Priority/" + winner, nil
	}
	return desired, "Baseline", nil
}

// priorityWinner returns the resident baseline workload that should own a
// pool. Lower values are more urgent; names provide deterministic tie-breaking.
// An active Pin override selects its target before priority comparison. This
// is automatic arbitration only: explicit safety blocks still win.
func (r *WorkloadReconciler) priorityWinner(ctx context.Context, namespace, poolRef string, overrides conductorv1alpha1.WorkloadOverrideList, now time.Time) (string, error) {
	var workloads conductorv1alpha1.ManagedWorkloadList
	if err := r.List(ctx, &workloads, client.InNamespace(namespace)); err != nil {
		return "", err
	}
	pinned := ""
	for _, override := range overrides.Items {
		if override.Spec.PoolRef == poolRef && override.Spec.Mode == "Pin" && override.Spec.WorkloadRef != "" && (override.Spec.ExpiresAt == nil || override.Spec.ExpiresAt.After(now)) {
			pinned = override.Spec.WorkloadRef
			break
		}
	}
	winner := ""
	winnerPriority := int32(0)
	for _, candidate := range workloads.Items {
		if candidate.Spec.PoolRef != poolRef || candidate.Spec.DesiredState != conductorv1alpha1.DesiredResident || candidate.Spec.Paused {
			continue
		}
		if pinned != "" && candidate.Name != pinned {
			continue
		}
		if winner == "" || candidate.Spec.Priority < winnerPriority || (candidate.Spec.Priority == winnerPriority && candidate.Name < winner) {
			winner = candidate.Name
			winnerPriority = candidate.Spec.Priority
		}
	}
	return winner, nil
}

func assess(members []observedMember, desired conductorv1alpha1.DesiredState) (bool, string) {
	for _, m := range members {
		if m.Deployment.Name == "" {
			return false, "DeploymentMissing"
		}
		if desired == conductorv1alpha1.DesiredResident {
			want := m.Spec.Replicas
			if want == 0 {
				want = 1
			}
			if ptrValue(m.Deployment.Spec.Replicas) != want || m.Deployment.Status.AvailableReplicas < want {
				return false, "MemberUnavailable"
			}
			// Availability of the current Deployment generation is authoritative.
			// Old ReplicaSet pods can remain Failed after a successful rollout and
			// must not poison an otherwise healthy workload forever.
		}
	}
	return true, "Observed"
}

func (r *WorkloadReconciler) startupInProgress(workload *conductorv1alpha1.ManagedWorkload, members []observedMember) bool {
	grace := workload.Spec.Driver.DeploymentSet.StartupGracePeriod.Duration
	if grace <= 0 {
		grace = defaultStartupGracePeriod
	}
	now := r.Clock.Now()
	for _, member := range members {
		want := member.Spec.Replicas
		if want == 0 {
			want = 1
		}
		if member.Deployment.Name == "" || ptrValue(member.Deployment.Spec.Replicas) != want || member.Deployment.Status.AvailableReplicas >= want {
			continue
		}
		for _, pod := range member.Pods {
			if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
				continue
			}
			started := pod.CreationTimestamp.Time
			if pod.Status.StartTime != nil && pod.Status.StartTime.After(started) {
				started = pod.Status.StartTime.Time
			}
			if now.Sub(started) >= 0 && now.Sub(started) < grace {
				return true
			}
		}
	}
	return false
}

func (r *WorkloadReconciler) nodeSafety(ctx context.Context, pool *conductorv1alpha1.ResourcePool, members []observedMember) (bool, string, error) {
	for _, member := range members {
		for _, pod := range member.Pods {
			if pod.Spec.NodeName == "" || pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
				continue
			}
			var node corev1.Node
			if err := r.APIReader.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, &node); err != nil {
				if apierrors.IsNotFound(err) {
					return r.fenceOrBlock(ctx, pool, pod.Spec.NodeName)
				}
				return false, "", err
			}
			ready := false
			for _, condition := range node.Status.Conditions {
				if condition.Type == corev1.NodeReady {
					ready = condition.Status == corev1.ConditionTrue
					break
				}
			}
			if !ready {
				return r.fenceOrBlock(ctx, pool, node.Name)
			}
		}
	}
	return false, "", nil
}

func (r *WorkloadReconciler) priorityPreemptionPending(ctx context.Context, workload *conductorv1alpha1.ManagedWorkload) (string, error) {
	var overrides conductorv1alpha1.WorkloadOverrideList
	if err := r.List(ctx, &overrides, client.InNamespace(workload.Namespace)); err != nil {
		return "", err
	}
	winner, err := r.priorityWinner(ctx, workload.Namespace, workload.Spec.PoolRef, overrides, r.Clock.Now())
	if err != nil || winner != workload.Name {
		return "", err
	}
	var workloads conductorv1alpha1.ManagedWorkloadList
	if err := r.List(ctx, &workloads, client.InNamespace(workload.Namespace)); err != nil {
		return "", err
	}
	for _, candidate := range workloads.Items {
		if candidate.Name == workload.Name || candidate.Spec.PoolRef != workload.Spec.PoolRef || candidate.Spec.DesiredState != conductorv1alpha1.DesiredResident || candidate.Spec.Paused {
			continue
		}
		if candidate.Spec.Priority < workload.Spec.Priority || (candidate.Spec.Priority == workload.Spec.Priority && candidate.Name < workload.Name) {
			continue
		}
		if candidate.Spec.Driver.DeploymentSet == nil {
			continue
		}
		for _, member := range candidate.Spec.Driver.DeploymentSet.Members {
			var deployment appsv1.Deployment
			if err := r.Get(ctx, types.NamespacedName{Namespace: candidate.Namespace, Name: member.Name}, &deployment); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return "", err
			}
			if ptrValue(deployment.Spec.Replicas) > 0 {
				return candidate.Name, nil
			}
		}
	}
	return "", nil
}

func (r *WorkloadReconciler) fenceOrBlock(ctx context.Context, pool *conductorv1alpha1.ResourcePool, nodeName string) (bool, string, error) {
	require := pool.Spec.Policy.RequireFencing == nil || *pool.Spec.Policy.RequireFencing
	if !require {
		return false, fmt.Sprintf("node %s is not Ready; fencing policy disabled", nodeName), nil
	}
	if r.Fencer == nil {
		return true, fmt.Sprintf("node %s is not Ready; fencing proof required", nodeName), nil
	}
	result, err := r.Fencer.EnsureFenced(ctx, nodeName)
	if err != nil {
		return true, fmt.Sprintf("node %s fencing failed: %v", nodeName, err), nil
	}
	now := r.Clock.Now()
	if !result.Verified || result.NodeName != nodeName || result.Evidence == "" || result.ExpiresAt.IsZero() || !result.ExpiresAt.After(now) {
		return true, fmt.Sprintf("node %s fencing receipt is not independently verified or has expired", nodeName), nil
	}
	return false, "fenced:" + result.Evidence, nil
}

func (r *WorkloadReconciler) reconcileResident(ctx context.Context, workload *conductorv1alpha1.ManagedWorkload, members []observedMember) (ctrl.Result, error) {
	if pending, err := r.priorityPreemptionPending(ctx, workload); err != nil {
		return ctrl.Result{}, err
	} else if pending != "" {
		setCondition(&workload.Status.Conditions, metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionTrue, Reason: "WaitingForPriorityPreemption", Message: "waiting for lower-priority workload " + pending + " to release pool capacity", ObservedGeneration: workload.Generation})
		return r.updateStatus(ctx, workload, PhaseObserving, ctrl.Result{RequeueAfter: 5 * time.Second})
	}
	phase := workload.Status.Phase
	healthy, reason := assess(members, conductorv1alpha1.DesiredResident)
	if (phase == "" || phase == PhaseReady || phase == PhaseObserving) && healthy {
		endpoints, err := r.readyEndpoints(ctx, workload)
		if err != nil {
			return ctrl.Result{}, err
		}
		workload.Status.ReadyEndpoints = int32(endpoints)
		if endpoints != 1 {
			return r.failStatus(ctx, workload, PhaseBlocked, "EndpointCardinality", fmt.Sprintf("stable service has %d ready endpoints; expected 1", endpoints), true)
		}
		if err := r.qualifyHealth(ctx, workload); err != nil {
			return r.failStatus(ctx, workload, PhaseBlocked, "HealthCheckFailed", err.Error(), true)
		}
		setCondition(&workload.Status.Conditions, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: "AllMembersReady", ObservedGeneration: workload.Generation})
		clearCondition(&workload.Status.Conditions, ConditionProgressing, "Idle", workload.Generation)
		clearCondition(&workload.Status.Conditions, ConditionBlocked, "Healthy", workload.Generation)
		clearCondition(&workload.Status.Conditions, ConditionDegraded, "Healthy", workload.Generation)
		return r.updateStatus(ctx, workload, PhaseReady, ctrl.Result{RequeueAfter: 30 * time.Second})
	}
	if (phase == "" || phase == PhaseReady || phase == PhaseObserving || phase == PhaseBlocked) && r.startupInProgress(workload, members) {
		setCondition(&workload.Status.Conditions, metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionTrue, Reason: "StartupInProgress", ObservedGeneration: workload.Generation})
		setCondition(&workload.Status.Conditions, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: "StartupInProgress", ObservedGeneration: workload.Generation})
		return r.updateStatus(ctx, workload, PhaseObserving, ctrl.Result{RequeueAfter: 15 * time.Second})
	}
	if phase == "" || phase == PhaseReady || phase == PhaseObserving || phase == PhaseBlocked {
		workload.Status.Operation = &conductorv1alpha1.OperationStatus{ID: fmt.Sprintf("%s-%d", workload.UID, r.Clock.Now().UnixNano()), Phase: PhaseStoppingHead, Reason: reason, StartedAt: metav1.NewTime(r.Clock.Now()), UpdatedAt: metav1.NewTime(r.Clock.Now())}
		r.event(workload, corev1.EventTypeWarning, "RecoveryStarted", "coordinated DeploymentSet recovery started: "+reason)
		return r.updateStatus(ctx, workload, PhaseStoppingHead, ctrl.Result{Requeue: true})
	}

	switch phase {
	case PhaseStoppingHead:
		head := headMember(members)
		if head == nil {
			return r.failStatus(ctx, workload, PhaseBlocked, "HeadMissing", "DeploymentSet requires one head member", true)
		}
		if err := r.scale(ctx, &head.Deployment, 0); err != nil {
			return ctrl.Result{}, err
		}
		return r.advance(ctx, workload, PhaseWaitingHeadGone, "")
	case PhaseWaitingHeadGone:
		head := headMember(members)
		if head != nil && !memberGone(*head) {
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}
		return r.advance(ctx, workload, PhaseStoppingWorkers, "")
	case PhaseStoppingWorkers:
		for _, worker := range workersByStop(members) {
			if ptrValue(worker.Deployment.Spec.Replicas) != 0 {
				if err := r.scale(ctx, &worker.Deployment, 0); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
		}
		return r.advance(ctx, workload, PhaseWaitingAllGone, "")
	case PhaseWaitingAllGone:
		for _, member := range members {
			if !memberGone(member) {
				return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
			}
		}
		return r.advance(ctx, workload, PhaseStartingWorkers, "")
	case PhaseStartingWorkers:
		for _, worker := range workersByStart(members) {
			want := worker.Spec.Replicas
			if want == 0 {
				want = 1
			}
			// The first unready worker is authoritative even if a prior scale
			// succeeded but persisting WaitingWorkerReady failed. Never skip an
			// unready rank and proceed to the next worker/head.
			if ptrValue(worker.Deployment.Spec.Replicas) != want {
				if err := r.scale(ctx, &worker.Deployment, want); err != nil {
					return ctrl.Result{}, err
				}
			}
			if !memberReady(worker) {
				workload.Status.Operation.CurrentMember = worker.Spec.Name
				return r.advance(ctx, workload, PhaseWaitingWorkerReady, worker.Spec.Name)
			}
		}
		return r.advance(ctx, workload, PhaseStartingHead, "")
	case PhaseWaitingWorkerReady:
		name := workload.Status.Operation.CurrentMember
		member := findMember(members, name)
		if member == nil || !memberReady(*member) {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		workload.Status.Operation.CurrentMember = ""
		return r.advance(ctx, workload, PhaseStartingWorkers, "")
	case PhaseStartingHead:
		head := headMember(members)
		if head == nil {
			return r.failStatus(ctx, workload, PhaseBlocked, "HeadMissing", "DeploymentSet requires one head member", true)
		}
		want := head.Spec.Replicas
		if want == 0 {
			want = 1
		}
		if ptrValue(head.Deployment.Spec.Replicas) != want {
			if err := r.scale(ctx, &head.Deployment, want); err != nil {
				return ctrl.Result{}, err
			}
		}
		return r.advance(ctx, workload, PhaseQualifying, head.Spec.Name)
	case PhaseQualifying:
		for _, member := range members {
			if !memberReady(member) {
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
		}
		endpoints, err := r.readyEndpoints(ctx, workload)
		if err != nil {
			return ctrl.Result{}, err
		}
		workload.Status.ReadyEndpoints = int32(endpoints)
		if endpoints != 1 {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		if err := r.qualifyHealth(ctx, workload); err != nil {
			setCondition(&workload.Status.Conditions, metav1.Condition{Type: ConditionDegraded, Status: metav1.ConditionTrue, Reason: "HealthCheckFailed", Message: err.Error(), ObservedGeneration: workload.Generation})
			_, _ = r.updateStatus(ctx, workload, PhaseQualifying, ctrl.Result{})
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		workload.Status.Operation = nil
		setCondition(&workload.Status.Conditions, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: "Qualified", ObservedGeneration: workload.Generation})
		clearCondition(&workload.Status.Conditions, ConditionProgressing, "Completed", workload.Generation)
		clearCondition(&workload.Status.Conditions, ConditionBlocked, "Healthy", workload.Generation)
		clearCondition(&workload.Status.Conditions, ConditionDegraded, "Healthy", workload.Generation)
		r.event(workload, corev1.EventTypeNormal, "RecoveryCompleted", "all members ready, direct health passed, and stable service has one ready endpoint")
		return r.updateStatus(ctx, workload, PhaseReady, ctrl.Result{RequeueAfter: 30 * time.Second})
	default:
		return r.failStatus(ctx, workload, PhaseBlocked, "UnknownPhase", phase, true)
	}
}

func (r *WorkloadReconciler) reconcileStandby(ctx context.Context, workload *conductorv1alpha1.ManagedWorkload, members []observedMember) (ctrl.Result, error) {
	// Standby follows the same safety ordering but intentionally stops after all
	// members are gone. This is the primitive future pool arbitration uses.
	head := headMember(members)
	if head != nil && ptrValue(head.Deployment.Spec.Replicas) != 0 {
		if err := r.scale(ctx, &head.Deployment, 0); err != nil {
			return ctrl.Result{}, err
		}
		return r.updateStatus(ctx, workload, PhaseWaitingHeadGone, ctrl.Result{RequeueAfter: 3 * time.Second})
	}
	if head != nil && !memberGone(*head) {
		return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
	}
	for _, worker := range workersByStop(members) {
		if ptrValue(worker.Deployment.Spec.Replicas) != 0 {
			if err := r.scale(ctx, &worker.Deployment, 0); err != nil {
				return ctrl.Result{}, err
			}
			return r.updateStatus(ctx, workload, PhaseWaitingAllGone, ctrl.Result{RequeueAfter: 3 * time.Second})
		}
		if !memberGone(worker) {
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}
	}
	setCondition(&workload.Status.Conditions, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: "Standby", ObservedGeneration: workload.Generation})
	return r.updateStatus(ctx, workload, "Standby", ctrl.Result{RequeueAfter: 30 * time.Second})
}

func (r *WorkloadReconciler) scale(ctx context.Context, dep *appsv1.Deployment, replicas int32) error {
	if dep.Name == "" {
		return fmt.Errorf("cannot scale a missing deployment")
	}
	fresh := &appsv1.Deployment{}
	if err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: dep.Namespace, Name: dep.Name}, fresh); err != nil {
		return err
	}
	if dep.UID != "" && fresh.UID != dep.UID {
		return fmt.Errorf("deployment %s identity changed; refusing stale scale", dep.Name)
	}
	if fresh.Generation != dep.Generation {
		return fmt.Errorf("deployment %s generation changed; re-observe before scaling", dep.Name)
	}
	scale := &autoscalingv1.Scale{}
	if err := r.SubResource("scale").Get(ctx, fresh, scale); err != nil {
		return err
	}
	if scale.Spec.Replicas == replicas {
		return nil
	}
	scale.Spec.Replicas = replicas
	return r.SubResource("scale").Update(ctx, fresh, client.WithSubResourceBody(scale))
}

func (r *WorkloadReconciler) qualifyHealth(ctx context.Context, workload *conductorv1alpha1.ManagedWorkload) error {
	q := workload.Spec.Qualification
	if q == nil || q.ServiceName == "" {
		return nil
	}
	path := q.Path
	if path == "" {
		path = "/health"
	}
	port := q.Port
	if port == 0 {
		port = 8000
	}
	checker := r.HealthChecker
	if checker == nil {
		checker = HTTPHealthChecker{}
	}
	url := fmt.Sprintf("http://%s.%s.svc:%d%s", q.ServiceName, workload.Namespace, port, path)
	return checker.Check(ctx, url)
}

func (r *WorkloadReconciler) readyEndpoints(ctx context.Context, workload *conductorv1alpha1.ManagedWorkload) (int, error) {
	service := ""
	if workload.Spec.Routing != nil {
		service = workload.Spec.Routing.ServiceName
	}
	if service == "" && workload.Spec.Qualification != nil {
		service = workload.Spec.Qualification.ServiceName
	}
	if service == "" {
		return 1, nil
	}
	var slices discoveryv1.EndpointSliceList
	if err := r.List(ctx, &slices, client.InNamespace(workload.Namespace), client.MatchingLabels{discoveryv1.LabelServiceName: service}); err != nil {
		return 0, err
	}
	n := 0
	for _, slice := range slices.Items {
		for _, endpoint := range slice.Endpoints {
			if endpoint.Conditions.Ready != nil && *endpoint.Conditions.Ready {
				n += len(endpoint.Addresses)
			}
		}
	}
	return n, nil
}

func (r *WorkloadReconciler) advance(ctx context.Context, workload *conductorv1alpha1.ManagedWorkload, phase, member string) (ctrl.Result, error) {
	if workload.Status.Operation == nil {
		workload.Status.Operation = &conductorv1alpha1.OperationStatus{}
	}
	workload.Status.Operation.Phase = phase
	workload.Status.Operation.UpdatedAt = metav1.NewTime(r.Clock.Now())
	if member != "" {
		workload.Status.Operation.CurrentMember = member
	}
	setCondition(&workload.Status.Conditions, metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionTrue, Reason: phase, ObservedGeneration: workload.Generation})
	clearCondition(&workload.Status.Conditions, ConditionReady, "Transitioning", workload.Generation)
	clearCondition(&workload.Status.Conditions, ConditionBlocked, "Transitioning", workload.Generation)
	return r.updateStatus(ctx, workload, phase, ctrl.Result{Requeue: true})
}

func (r *WorkloadReconciler) failStatus(ctx context.Context, workload *conductorv1alpha1.ManagedWorkload, phase, reason, message string, blocked bool) (ctrl.Result, error) {
	condition := ConditionDegraded
	if blocked {
		condition = ConditionBlocked
	}
	setCondition(&workload.Status.Conditions, metav1.Condition{Type: condition, Status: metav1.ConditionTrue, Reason: reason, Message: message, ObservedGeneration: workload.Generation})
	clearCondition(&workload.Status.Conditions, ConditionReady, reason, workload.Generation)
	clearCondition(&workload.Status.Conditions, ConditionProgressing, reason, workload.Generation)
	return r.updateStatus(ctx, workload, phase, ctrl.Result{RequeueAfter: 30 * time.Second})
}

func (r *WorkloadReconciler) updateStatus(ctx context.Context, workload *conductorv1alpha1.ManagedWorkload, phase string, result ctrl.Result) (ctrl.Result, error) {
	workload.Status.Phase = phase
	var latest conductorv1alpha1.ManagedWorkload
	if err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: workload.Namespace, Name: workload.Name}, &latest); err != nil {
		return ctrl.Result{}, err
	}
	if equality.Semantic.DeepEqual(latest.Status, workload.Status) {
		return result, nil
	}
	latest.Status = workload.Status
	if err := r.Status().Update(ctx, &latest); err != nil {
		return ctrl.Result{}, err
	}
	return result, nil
}

func (r *WorkloadReconciler) event(workload *conductorv1alpha1.ManagedWorkload, eventType, reason, message string) {
	if r.Recorder != nil {
		r.Recorder.Event(workload, eventType, reason, message)
	}
}

func setCondition(conditions *[]metav1.Condition, condition metav1.Condition) {
	condition.LastTransitionTime = metav1.Now()
	apimeta.SetStatusCondition(conditions, condition)
}

func clearCondition(conditions *[]metav1.Condition, conditionType, reason string, generation int64) {
	setCondition(conditions, metav1.Condition{Type: conditionType, Status: metav1.ConditionFalse, Reason: reason, ObservedGeneration: generation})
}

func boolCondition(v bool) metav1.ConditionStatus {
	if v {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}
func ptrValue(p *int32) int32 {
	if p == nil {
		return 1
	}
	return *p
}
func memberGone(m observedMember) bool {
	activePods := 0
	for _, pod := range m.Pods {
		if pod.Status.Phase != corev1.PodFailed && pod.Status.Phase != corev1.PodSucceeded {
			activePods++
		}
	}
	return ptrValue(m.Deployment.Spec.Replicas) == 0 && m.Deployment.Status.Replicas == 0 && activePods == 0
}
func memberReady(m observedMember) bool {
	want := m.Spec.Replicas
	if want == 0 {
		want = 1
	}
	return ptrValue(m.Deployment.Spec.Replicas) == want &&
		m.Deployment.Status.ObservedGeneration >= m.Deployment.Generation &&
		m.Deployment.Status.UpdatedReplicas >= want &&
		m.Deployment.Status.AvailableReplicas >= want
}
func findMember(m []observedMember, name string) *observedMember {
	for i := range m {
		if m[i].Spec.Name == name {
			return &m[i]
		}
	}
	return nil
}
func headMember(m []observedMember) *observedMember {
	for i := range m {
		if m[i].Spec.Role == "head" {
			return &m[i]
		}
	}
	return nil
}
func workersByStart(m []observedMember) []observedMember {
	out := []observedMember{}
	for _, x := range m {
		if x.Spec.Role != "head" {
			out = append(out, x)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Spec.StartOrder < out[j].Spec.StartOrder })
	return out
}
func workersByStop(m []observedMember) []observedMember {
	out := []observedMember{}
	for _, x := range m {
		if x.Spec.Role != "head" {
			out = append(out, x)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Spec.StopOrder < out[j].Spec.StopOrder })
	return out
}

func (r *WorkloadReconciler) mapDeployment(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.mapNamespace(ctx, obj.GetNamespace(), obj.GetName())
}

func (r *WorkloadReconciler) mapAll(ctx context.Context, _ client.Object) []reconcile.Request {
	var workloads conductorv1alpha1.ManagedWorkloadList
	if err := r.List(ctx, &workloads); err != nil {
		return nil
	}
	out := make([]reconcile.Request, 0, len(workloads.Items))
	for _, workload := range workloads.Items {
		out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: workload.Namespace, Name: workload.Name}})
	}
	return out
}

func (r *WorkloadReconciler) mapNamespace(ctx context.Context, namespace, deployment string) []reconcile.Request {
	var workloads conductorv1alpha1.ManagedWorkloadList
	if err := r.List(ctx, &workloads, client.InNamespace(namespace)); err != nil {
		return nil
	}
	var out []reconcile.Request
	for _, workload := range workloads.Items {
		if workload.Spec.Driver.DeploymentSet == nil {
			continue
		}
		for _, member := range workload.Spec.Driver.DeploymentSet.Members {
			if deployment == "" || member.Name == deployment {
				out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: workload.Name}})
				break
			}
		}
	}
	return out
}

func (r *WorkloadReconciler) SetupWithManager(mgr ctrl.Manager) error {
	mapNamespace := handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
		return r.mapNamespace(ctx, obj.GetNamespace(), "")
	})
	return ctrl.NewControllerManagedBy(mgr).
		For(&conductorv1alpha1.ManagedWorkload{}).
		Watches(&conductorv1alpha1.ManagedWorkload{}, handler.EnqueueRequestsFromMapFunc(r.mapAll)).
		Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(r.mapDeployment)).
		Watches(&corev1.Pod{}, mapNamespace).
		Watches(&discoveryv1.EndpointSlice{}, mapNamespace).
		Watches(&conductorv1alpha1.ResourcePool{}, mapNamespace).
		Watches(&conductorv1alpha1.WorkloadOverride{}, mapNamespace).
		Watches(&corev1.Node{}, handler.EnqueueRequestsFromMapFunc(r.mapAll)).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}
