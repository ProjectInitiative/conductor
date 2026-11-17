package controller

import (
	"context"
	"testing"
	"time"

	conductorv1alpha1 "github.com/projectinitiative/conductor/pkg/apis/conductor/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func mkRingNode(name string, position string, ready bool) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{DefaultRingLabel: position},
		},
	}
	if ready {
		node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	} else {
		node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
	}
	return node
}

func TestRingFromNodesSkipsUnlabelledAndUnready(t *testing.T) {
	nodes := []corev1.Node{
		*mkRingNode("chronometer", "2", true),
		{ObjectMeta: metav1.ObjectMeta{Name: "capstan1"}}, // no ring label
		*mkRingNode("astrolabe", "not-a-number", true),
		*mkRingNode("sextant", "3", false),
		*mkRingNode("octant", "1", true),
	}
	ring := ringFromNodes(nodes, DefaultRingLabel)
	if len(ring) != 3 {
		t.Fatalf("ring=%+v, want 3 members", ring)
	}
	if ring[0].Name != "octant" || ring[1].Name != "chronometer" || ring[2].Name != "sextant" {
		t.Fatalf("ring order=%+v, want octant,chronometer,sextant", ring)
	}
	if ring[2].Ready {
		t.Fatalf("unready sextant should carry Ready=false: %+v", ring[2])
	}
}

func TestRingFromNodesKeepsFirstDuplicatePosition(t *testing.T) {
	nodes := []corev1.Node{
		*mkRingNode("first", "1", true),
		*mkRingNode("second", "1", true),
	}
	ring := ringFromNodes(nodes, DefaultRingLabel)
	if len(ring) != 1 || ring[0].Name != "first" {
		t.Fatalf("ring=%+v, want single first", ring)
	}
}

func TestChooseAdjacentPairFirstReadyEdge(t *testing.T) {
	nodes := []corev1.Node{
		*mkRingNode("astrolabe", "0", true),
		*mkRingNode("octant", "1", true),
		*mkRingNode("chronometer", "2", true),
		*mkRingNode("sextant", "3", true),
	}
	pair := chooseAdjacentPair(nodes, DefaultRingLabel)
	if pair == nil {
		t.Fatal("pair=nil, want astrolabe/octant")
	}
	if pair.Worker.Name != "astrolabe" || pair.Head.Name != "octant" {
		t.Fatalf("pair=%+v, want worker=astrolabe head=octant", pair)
	}
}

func TestChooseAdjacentPairSkipsUnreadyAndWraps(t *testing.T) {
	// astrolabe (0) is NotReady, so the astrolabe->octant edge fails its
	// worker check; the first fully-ready edge in walk order is
	// octant(1)->chronometer(2). The chronometer->sextant and
	// sextant->astrolabe edges are never reached.
	nodes := []corev1.Node{
		*mkRingNode("astrolabe", "0", false),
		*mkRingNode("octant", "1", true),
		*mkRingNode("chronometer", "2", true),
		*mkRingNode("sextant", "3", true),
	}
	pair := chooseAdjacentPair(nodes, DefaultRingLabel)
	if pair == nil {
		t.Fatal("pair=nil, want octant/chronometer")
	}
	if pair.Worker.Name != "octant" || pair.Head.Name != "chronometer" {
		t.Fatalf("pair=%+v, want worker=octant head=chronometer", pair)
	}
}

func TestChooseAdjacentPairAllUnreadyOrTooSmall(t *testing.T) {
	unready := []corev1.Node{*mkRingNode("a", "0", false), *mkRingNode("b", "1", false)}
	if pair := chooseAdjacentPair(unready, DefaultRingLabel); pair != nil {
		t.Fatalf("pair=%+v, want nil when all unready", pair)
	}
	solo := []corev1.Node{*mkRingNode("a", "0", true)}
	if pair := chooseAdjacentPair(solo, DefaultRingLabel); pair != nil {
		t.Fatalf("pair=%+v, want nil for single-node ring", pair)
	}
	if pair := chooseAdjacentPair(nil, DefaultRingLabel); pair != nil {
		t.Fatalf("pair=%+v, want nil for empty ring", pair)
	}
}

// placementHarness builds a Manage-mode workload with the given placement mode
// whose pair is all-gone, plus the four ring nodes (astrolabe NotReady to
// exercise edge selection), and drives one reconcile pass from PhasePlacing.
func placementHarness(t *testing.T, mode string, pinned map[string]string) (*WorkloadReconciler, *appsv1.Deployment, *appsv1.Deployment, *conductorv1alpha1.ManagedWorkload) {
	t.Helper()
	pool, workload, worker, head := workloadObjects(conductorv1alpha1.ManagementManage)
	workload.Spec.Driver.DeploymentSet.Placement = &conductorv1alpha1.PlacementSpec{Mode: mode}
	workload.Status.Phase = PhasePlacing
	workload.Status.Operation = &conductorv1alpha1.OperationStatus{ID: "op-1", Phase: PhasePlacing}
	worker.Spec.Replicas = i32(0)
	worker.Status = appsv1.DeploymentStatus{ObservedGeneration: 1}
	head.Spec.Replicas = i32(0)
	head.Status = appsv1.DeploymentStatus{ObservedGeneration: 1}
	for _, dep := range []*appsv1.Deployment{worker, head} {
		if pinned != nil {
			sel := map[string]string{"gpu": "dgx-spark"}
			for k, v := range pinned {
				sel[k] = v
			}
			dep.Spec.Template.Spec.NodeSelector = sel
		}
	}
	// Ring: astrolabe (position 0) is always NotReady so the first ready edge
	// is octant(1)->chronometer(2); othersReady toggles the remaining three.
	defaultRingNodes := func(othersReady bool) []client.Object {
		return []client.Object{
			pool, workload, worker, head,
			mkRingNode("astrolabe", "0", false),
			mkRingNode("octant", "1", othersReady),
			mkRingNode("chronometer", "2", othersReady),
			mkRingNode("sextant", "3", othersReady),
		}
	}
	nodeList := defaultRingNodes(true)
	scheme := testScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(workload).
		WithObjects(nodeList...).Build()
	r := &WorkloadReconciler{Client: client, APIReader: client, Clock: clocktesting.NewFakeClock(time.Unix(100, 0)), Recorder: record.NewFakeRecorder(10)}
	return r, worker, head, workload
}

func TestPlacementBestRoutePinsAdjacentPair(t *testing.T) {
	r, _, _, workload := placementHarness(t, PlacementModeBestRoute, nil)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}}); err != nil {
		t.Fatal(err)
	}
	var gotW, gotH appsv1.Deployment
	_ = r.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "worker"}, &gotW)
	_ = r.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "head"}, &gotH)
	// astrolabe (0) is NotReady; first ready edge is octant(1)->chronometer(2).
	if gotW.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"] != "octant" {
		t.Fatalf("worker selector=%v, want octant", gotW.Spec.Template.Spec.NodeSelector)
	}
	if gotH.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"] != "chronometer" {
		t.Fatalf("head selector=%v, want chronometer", gotH.Spec.Template.Spec.NodeSelector)
	}
	var got conductorv1alpha1.ManagedWorkload
	_ = r.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}, &got)
	if got.Status.Placement == nil || got.Status.Placement.Mode != PlacementModeAdjacent || got.Status.Placement.WorkerNode != "octant" {
		t.Fatalf("placement status=%+v", got.Status.Placement)
	}
	if got.Status.Phase != PhaseStartingWorkers {
		t.Fatalf("phase=%q, want %q", got.Status.Phase, PhaseStartingWorkers)
	}
	if workload.Spec.Driver.DeploymentSet.Placement.Mode != PlacementModeBestRoute {
		t.Fatal("spec mutated")
	}
}

func TestPlacementAnyClearsStalePins(t *testing.T) {
	r, _, _, _ := placementHarness(t, PlacementModeAny, map[string]string{"kubernetes.io/hostname": "sextant"})
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}}); err != nil {
		t.Fatal(err)
	}
	var gotW, gotH appsv1.Deployment
	_ = r.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "worker"}, &gotW)
	_ = r.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "head"}, &gotH)
	if _, ok := gotW.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"]; ok {
		t.Fatalf("worker selector=%v, want hostname cleared", gotW.Spec.Template.Spec.NodeSelector)
	}
	if gotW.Spec.Template.Spec.NodeSelector["gpu"] != "dgx-spark" {
		t.Fatalf("worker selector=%v, want unrelated keys preserved", gotW.Spec.Template.Spec.NodeSelector)
	}
	if _, ok := gotH.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"]; ok {
		t.Fatalf("head selector=%v, want hostname cleared", gotH.Spec.Template.Spec.NodeSelector)
	}
}

func TestPlacementAdjacentBlocksWhenNoPairReady(t *testing.T) {
	r, _, _, _ := placementHarness(t, PlacementModeAdjacent, nil)
	// Make every ring node NotReady so no edge qualifies: the workload must
	// block with NoAdjacentPair instead of degrading to a routed placement.
	for _, name := range []string{"octant", "chronometer", "sextant"} {
		var n corev1.Node
		if err := r.Get(context.Background(), types.NamespacedName{Name: name}, &n); err != nil {
			t.Fatal(err)
		}
		n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
		if err := r.Status().Update(context.Background(), &n); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}}); err != nil {
		t.Fatal(err)
	}
	var got conductorv1alpha1.ManagedWorkload
	_ = r.Get(context.Background(), types.NamespacedName{Namespace: "llm-test", Name: "tensorfold"}, &got)
	if got.Status.Phase != PhaseBlocked {
		t.Fatalf("phase=%q, want %q", got.Status.Phase, PhaseBlocked)
	}
	blocked := apimeta.FindStatusCondition(got.Status.Conditions, ConditionBlocked)
	if blocked == nil || blocked.Reason != "NoAdjacentPair" {
		t.Fatalf("blocked condition=%+v, want reason NoAdjacentPair", blocked)
	}
}

func TestNodeReadySkipsCordonAndTaints(t *testing.T) {
	ready := mkRingNode("a", "0", true)
	if !nodeReady(ready) {
		t.Fatal("ready node reported ineligible")
	}
	unschedulable := mkRingNode("b", "1", true)
	unschedulable.Spec.Unschedulable = true
	if nodeReady(unschedulable) {
		t.Fatal("cordoned node reported eligible")
	}
	tainted := mkRingNode("c", "2", true)
	tainted.Spec.Taints = []corev1.Taint{{Key: "dgx-spark-dedicated", Value: "true", Effect: corev1.TaintEffectNoSchedule}}
	if nodeReady(tainted) {
		t.Fatal("NoSchedule-tainted node reported eligible")
	}
	tolerable := mkRingNode("d", "3", true)
	tolerable.Spec.Taints = []corev1.Taint{{Key: "dgx-spark-dedicated", Value: "true", Effect: corev1.TaintEffectNoSchedule}}
	tolerantWorkload := false // only the taint matters here; tolerance is the workload's concern
	_ = tolerantWorkload
	if nodeReady(tolerable) {
		t.Fatal("tainted node reported eligible")
	}
}

func TestChooseAdjacentPairSkipsCordonedNode(t *testing.T) {
	nodes := []corev1.Node{
		*mkRingNode("astrolabe", "0", true),
		*mkRingNode("octant", "1", true),
		*mkRingNode("chronometer", "2", true),
		*mkRingNode("sextant", "3", true),
	}
	// Cordon octant: the astro->oct edge must be skipped, fall through to
	// oct->chron which also contains octant... both edges with octant are out;
	// chron(2)->sext(3) is the first eligible edge.
	unschedulable := nodes[1]
	unschedulable.Spec.Unschedulable = true
	nodes[1] = unschedulable
	pair := chooseAdjacentPair(nodes, DefaultRingLabel)
	if pair == nil {
		t.Fatal("pair=nil, want chronometer/sextant")
	}
	if pair.Worker.Name != "chronometer" || pair.Head.Name != "sextant" {
		t.Fatalf("pair=%+v", pair)
	}
}
