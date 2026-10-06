package operator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func mkLane(name string, ranks ...*RankInfo) *Lane {
	l := &Lane{Name: name, Namespace: "llm-test"}
	l.Ranks = ranks
	return l
}

func mkRank(name string, rank int, replicas, current, ready int32) *RankInfo {
	return &RankInfo{Name: name, Rank: rank, Replicas: replicas, Current: current, Ready: ready}
}

func TestRankOfSuffixAndArgs(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want int
	}{
		{"glm53-full-head", nil, 0},
		{"glm53-full-worker1", nil, 1},
		{"glm53-full-worker2", nil, 2},
		{"tensorfold-v14-worker", nil, 1},
		{"glm53-rank0", []string{"--rank", "0"}, 0},
		{"glm53-rankX", []string{"--rank=3"}, 3},
		{"weird", nil, 1},
	}
	for _, c := range cases {
		if got := RankOf(c.name, c.args); got != c.want {
			t.Errorf("RankOf(%q, %v) = %d, want %d", c.name, c.args, got, c.want)
		}
	}
}

func TestLaneNameOf(t *testing.T) {
	if got := LaneNameOf("tensorfold-v14-head", "tensorfold-v14"); got != "tensorfold-v14" {
		t.Errorf("lane label should win: got %q", got)
	}
	if got := LaneNameOf("glm53-full-worker2", ""); got != "glm53-full" {
		t.Errorf("suffix derive: got %q", got)
	}
	if got := LaneNameOf("solo", ""); got != "solo" {
		t.Errorf("bare name: got %q", got)
	}
}

func TestOrdering(t *testing.T) {
	lane := mkLane("glm53-full",
		mkRank("glm53-full-head", 0, 0, 0, 0),
		mkRank("glm53-full-worker1", 1, 0, 0, 0),
		mkRank("glm53-full-worker2", 2, 0, 0, 0),
	)
	up := lane.UpOrder()
	if up[0].Name != "glm53-full-worker2" || up[1].Name != "glm53-full-worker1" || up[2].Name != "glm53-full-head" {
		t.Errorf("UpOrder must be worker2→worker1→head, got %v %v %v", up[0].Name, up[1].Name, up[2].Name)
	}
	down := lane.DownOrder()
	if down[0].Name != "glm53-full-head" || down[2].Name != "glm53-full-worker2" {
		t.Errorf("DownOrder must be head-first, got %v %v %v", down[0].Name, down[1].Name, down[2].Name)
	}
	if lane.Head().Name != "glm53-full-head" {
		t.Errorf("Head() = %v", lane.Head().Name)
	}
}

func mkPod(name, owner string, terminating bool, node string, waitReason string, phase string) Pod {
	p := Pod{}
	p.Metadata.Name = name
	p.Metadata.OwnerReferences = []OwnerRef{{Kind: "ReplicaSet", Name: owner + "-rs"}}
	p.Spec.NodeName = node
	p.Status.Phase = phase
	if terminating {
		ts := "2026-10-06T13:44:34Z"
		p.Metadata.DeletionTimestamp = &ts
	}
	if waitReason != "" {
		p.Status.ContainerStatuses = []struct {
			Name  string         `json:"name"`
			State ContainerState `json:"state"`
		}{
			{Name: "tensorfold", State: ContainerState{Waiting: &struct {
				Reason string `json:"reason"`
			}{Reason: waitReason}}},
		}
	}
	return p
}

func ownersOf(pods ...Pod) map[string]string {
	// Mirror DeploymentOwners' shape: pod -> deployment.
	m := map[string]string{}
	for _, p := range pods {
		m[p.Metadata.Name] = strings.TrimSuffix(p.Metadata.OwnerReferences[0].Name, "-rs")
	}
	return m
}

func TestAssessLaneHealthy(t *testing.T) {
	lane := mkLane("tensorfold-v14",
		mkRank("tensorfold-v14-head", 0, 1, 1, 1),
		mkRank("tensorfold-v14-worker", 1, 1, 1, 1),
	)
	pods := []Pod{mkPod("h", "tensorfold-v14-head", false, "chronometer", "", "Running")}
	a := AssessLane(lane, pods, ownersOf(pods...), map[string]bool{"chronometer": true})
	if !a.Healthy || a.Recreate || len(a.ForceDeletes) != 0 {
		t.Errorf("expected healthy lane, got %+v", a)
	}
}

func TestAssessLaneStuckTerminating(t *testing.T) {
	lane := mkLane("tensorfold-v14",
		mkRank("tensorfold-v14-head", 0, 1, 1, 1),
		mkRank("tensorfold-v14-worker", 1, 1, 1, 1),
	)
	pods := []Pod{
		mkPod("h", "tensorfold-v14-head", false, "chronometer", "", "Running"),
		mkPod("w", "tensorfold-v14-worker", true, "octant", "", "Running"),
	}
	a := AssessLane(lane, pods, ownersOf(pods...), map[string]bool{"chronometer": true, "octant": true})
	if a.Recreate {
		t.Errorf("terminating pod alone must not trigger recreate: %+v", a)
	}
	if len(a.ForceDeletes) != 1 || a.ForceDeletes[0] != "w" {
		t.Errorf("expected force-delete of stuck worker, got %+v", a.ForceDeletes)
	}
	if a.Healthy {
		t.Errorf("lane with stuck pod is not healthy")
	}
}

func TestAssessLaneCrashLoopRecreates(t *testing.T) {
	lane := mkLane("tensorfold-v14",
		mkRank("tensorfold-v14-head", 0, 1, 1, 1),
		mkRank("tensorfold-v14-worker", 1, 1, 1, 1),
	)
	pods := []Pod{
		mkPod("h", "tensorfold-v14-head", false, "chronometer", "", "Running"),
		mkPod("w", "tensorfold-v14-worker", false, "octant", "CrashLoopBackOff", "Running"),
	}
	a := AssessLane(lane, pods, ownersOf(pods...), map[string]bool{"chronometer": true, "octant": true})
	if !a.Recreate {
		t.Errorf("CrashLoopBackOff must trigger coordinated recreation: %+v", a)
	}
	if !strings.Contains(a.Reason, "CrashLoopBackOff") {
		t.Errorf("reason should name the cause: %q", a.Reason)
	}
}

func TestAssessLaneOrphanRank(t *testing.T) {
	lane := mkLane("tensorfold-v14",
		mkRank("tensorfold-v14-head", 0, 1, 0, 0),
		mkRank("tensorfold-v14-worker", 1, 1, 1, 1),
	)
	a := AssessLane(lane, nil, map[string]string{}, map[string]bool{"chronometer": true})
	if !a.Recreate {
		t.Errorf("rank with replicas but zero pods must recreate: %+v", a)
	}
}

func TestAssessLanePodOnDeadNode(t *testing.T) {
	lane := mkLane("tensorfold-v14",
		mkRank("tensorfold-v14-head", 0, 1, 1, 1),
		mkRank("tensorfold-v14-worker", 1, 1, 1, 1),
	)
	pods := []Pod{mkPod("w", "tensorfold-v14-worker", false, "octant", "", "Running")}
	a := AssessLane(lane, pods, ownersOf(pods...), map[string]bool{"octant": false})
	if len(a.ForceDeletes) != 1 {
		t.Errorf("pod on NotReady node should be force-cleared: %+v", a)
	}
}

// dryKube returns a dry-run Kube that records plans without a cluster.
func dryKube() *Kube {
	k := New("", true)
	k.Runner = errRunner{}
	return k
}

type errRunner struct{}

func (errRunner) Run(_ context.Context, _ []string) ([]byte, error) {
	return nil, errTest
}

var errTest = fmt.Errorf("no cluster in unit tests")

func TestDeployDryRunPlanOrder(t *testing.T) {
	k := dryKube()
	lanes := map[string]*Lane{
		"glm53-full": mkLane("glm53-full",
			mkRank("glm53-full-head", 0, 0, 0, 0),
			mkRank("glm53-full-worker1", 1, 0, 0, 0),
			mkRank("glm53-full-worker2", 2, 0, 0, 0),
		),
	}
	opts := Options{Out: &strings.Builder{}, PerRankWait: 30 * time.Second}
	if err := Deploy(k, lanes, "glm53-full", opts); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	joined := strings.Join(k.Plan, "\n")
	i2 := strings.Index(joined, "glm53-full-worker2")
	i1 := strings.Index(joined, "glm53-full-worker1")
	i0 := strings.Index(joined, "glm53-full-head")
	if i2 < 0 || i1 < 0 || i0 < 0 || !(i2 < i1 && i1 < i0) {
		t.Errorf("plan must scale worker2→worker1→head; plan:\n%s", joined)
	}
}

func TestDeployRefusesWhileOtherLaneActive(t *testing.T) {
	k := dryKube()
	resHead := mkRank("resident-head", 0, 1, 1, 1)
	resHead.AliasValue = "dgx-spark"
	lanes := map[string]*Lane{
		"resident": mkLane("resident", resHead,
			mkRank("resident-worker", 1, 1, 1, 1),
		),
		"target": mkLane("target",
			mkRank("target-head", 0, 0, 0, 0),
		),
	}
	opts := Options{Out: &strings.Builder{}}
	err := Deploy(k, lanes, "target", opts)
	if err == nil || !strings.Contains(err.Error(), "switch") {
		t.Fatalf("deploy must refuse while another lane is active, got %v", err)
	}
}

func TestDeployAllowsScaledDownResident(t *testing.T) {
	k := dryKube()
	lanes := map[string]*Lane{
		"resident": mkLane("resident", mkRank("resident-head", 0, 0, 0, 0)),
		"target":   mkLane("target", mkRank("target-head", 0, 0, 0, 0)),
	}
	opts := Options{Out: &strings.Builder{}}
	if err := Deploy(k, lanes, "target", opts); err != nil {
		t.Fatalf("deploy over a scaled-down resident must proceed: %v", err)
	}
}

func TestMoveAliasPatchBodies(t *testing.T) {
	k := dryKube()
	oldHead := mkRank("old-head", 0, 0, 0, 0)
	oldHead.AliasValue = "dgx-spark"
	newHead := mkRank("new-head", 0, 1, 1, 1)
	lanes := map[string]*Lane{
		"old":   mkLane("old", oldHead),
		"new":   mkLane("new", newHead),
		"third": mkLane("third", mkRank("third-head", 0, 0, 0, 0)),
	}
	opts := Options{Out: &strings.Builder{}}
	if err := MoveAlias(k, lanes, "new", opts); err != nil {
		t.Fatalf("MoveAlias: %v", err)
	}
	joined := strings.Join(k.Plan, "\n")
	if !strings.Contains(joined, `"serving.homelab.io/alias":null`) {
		t.Errorf("old holder must lose the label (null merge patch); plan:\n%s", joined)
	}
	if !strings.Contains(joined, `"serving.homelab.io/alias":"dgx-spark"`) {
		t.Errorf("new holder must gain the label; plan:\n%s", joined)
	}
	if strings.Contains(joined, "third-head") {
		t.Errorf("unaffected lane must not be patched; plan:\n%s", joined)
	}
}
