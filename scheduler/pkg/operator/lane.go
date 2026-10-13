package operator

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

// House conventions (see homelab AGENTS.md, DEC-0023/0026, RISK-0018):
//   - Lanes are Deployments in llm-test labeled lane=<name> (or named
//     <lane>-head / <lane>-worker[1-9] when the label is absent).
//   - Rank order is the contract: bring ranks up from highest rank to the
//     head (worker2 -> worker1 -> head); bring them down head first. TP NCCL
//     pairs are a single failure domain — never restart one rank alone.
//   - Serving follows the stable alias: the head whose pod template carries
//     serving.homelab.io/alias=<alias> is resident; the stable Service
//     (dgx-spark-backend) selects on that label.
const (
	DefaultNamespace     = "llm-test"
	DefaultAliasLabel    = "serving.homelab.io/alias"
	DefaultAliasValue    = "dgx-spark"
	DefaultStableService = "dgx-spark-backend"
)

// Lane is one swappable model lane: its rank Deployments plus resolved ranks.
type Lane struct {
	Name      string
	Namespace string
	Ranks     []*RankInfo // sorted ascending by Rank
}

type RankInfo struct {
	Name       string
	Rank       int
	Replicas   int32 // spec
	Current    int32 // status.replicas
	Ready      int32
	AliasValue string // non-empty when this rank's pod template holds the alias label
}

// UpOrder returns ranks in activation order (highest rank first, head last).
func (l *Lane) UpOrder() []*RankInfo {
	out := make([]*RankInfo, len(l.Ranks))
	copy(out, l.Ranks)
	sort.Slice(out, func(i, j int) bool { return out[i].Rank > out[j].Rank })
	return out
}

// DownOrder returns ranks in shutdown order (head first).
func (l *Lane) DownOrder() []*RankInfo {
	out := make([]*RankInfo, len(l.Ranks))
	copy(out, l.Ranks)
	sort.Slice(out, func(i, j int) bool { return out[i].Rank < out[j].Rank })
	return out
}

func (l *Lane) Head() *RankInfo {
	for _, r := range l.Ranks {
		if r.Rank == 0 {
			return r
		}
	}
	return nil
}

// LaneNameOf derives the lane name for a Deployment.
func LaneNameOf(deployName, laneLabel string) string {
	if laneLabel != "" {
		return laneLabel
	}
	for _, suf := range []string{"-worker3", "-worker2", "-worker1", "-worker", "-head"} {
		if strings.HasSuffix(deployName, suf) {
			return strings.TrimSuffix(deployName, suf)
		}
	}
	return deployName
}

// RankOf resolves a rank number from container args (--rank N) or the
// deployment name suffix. Unknown non-head names default to rank 1.
func RankOf(deployName string, args []string) int {
	if n, ok := rankFromArgs(args); ok {
		return n
	}
	switch {
	case strings.HasSuffix(deployName, "-head"):
		return 0
	case strings.HasSuffix(deployName, "-worker3"):
		return 3
	case strings.HasSuffix(deployName, "-worker2"):
		return 2
	case strings.HasSuffix(deployName, "-worker1"):
		return 1
	case strings.HasSuffix(deployName, "-worker"):
		return 1
	}
	return 1
}

func rankFromArgs(args []string) (int, bool) {
	for i, a := range args {
		if a == "--rank" && i+1 < len(args) {
			if n, err := strconv.Atoi(args[i+1]); err == nil {
				return n, true
			}
		}
		if strings.HasPrefix(a, "--rank=") {
			if n, err := strconv.Atoi(strings.TrimPrefix(a, "--rank=")); err == nil {
				return n, true
			}
		}
	}
	return 0, false
}

// Options configures the operator verbs.
type Options struct {
	Namespace     string
	AliasLabel    string
	AliasValue    string
	StableService string
	PerRankWait   time.Duration
	Out           io.Writer
}

func (o *Options) Defaults() {
	if o.Namespace == "" {
		o.Namespace = DefaultNamespace
	}
	if o.AliasLabel == "" {
		o.AliasLabel = DefaultAliasLabel
	}
	if o.AliasValue == "" {
		o.AliasValue = DefaultAliasValue
	}
	if o.StableService == "" {
		o.StableService = DefaultStableService
	}
	if o.PerRankWait <= 0 {
		o.PerRankWait = 45 * time.Minute
	}
	if o.Out == nil {
		o.Out = io.Discard
	}
}

func (o *Options) logf(format string, args ...any) {
	fmt.Fprintf(o.Out, "==> "+format+"\n", args...)
}

// Discover groups the namespace's Deployments into lanes by lane label or
// name suffix, resolving rank from args/name and recording the alias holder.
func Discover(k *Kube, ns, aliasLabel string) (map[string]*Lane, error) {
	deps, err := k.ListDeployments(ns)
	if err != nil {
		return nil, err
	}
	lanes := map[string]*Lane{}
	for i := range deps {
		d := deps[i]
		laneName := LaneNameOf(d.Metadata.Name, d.Metadata.Labels["lane"])
		lane := lanes[laneName]
		if lane == nil {
			lane = &Lane{Name: laneName, Namespace: ns}
			lanes[laneName] = lane
		}
		ri := &RankInfo{
			Name:       d.Metadata.Name,
			Rank:       RankOf(d.Metadata.Name, d.RankArgs()),
			Replicas:   d.Spec.Replicas,
			Current:    d.Status.Replicas,
			Ready:      d.Status.ReadyReplicas,
			AliasValue: d.Spec.Template.Metadata.Labels[aliasLabel],
		}
		lane.Ranks = append(lane.Ranks, ri)
	}
	for _, lane := range lanes {
		sort.Slice(lane.Ranks, func(i, j int) bool { return lane.Ranks[i].Rank < lane.Ranks[j].Rank })
	}
	return lanes, nil
}

func GetLane(lanes map[string]*Lane, name string) (*Lane, error) {
	lane, ok := lanes[name]
	if !ok {
		names := make([]string, 0, len(lanes))
		for n := range lanes {
			names = append(names, n)
		}
		sort.Strings(names)
		return nil, fmt.Errorf("lane %q not found (known lanes: %s)", name, strings.Join(names, ", "))
	}
	return lane, nil
}

// residentActive reports whether any rank of the lane still has replicas.
func residentActive(lane *Lane) bool {
	if lane == nil {
		return false
	}
	for _, r := range lane.Ranks {
		if r.Replicas > 0 || r.Current > 0 {
			return true
		}
	}
	return false
}

// AliasHolder returns the lane whose rank-0 deployment pod template carries
// the alias label, or "".
func AliasHolder(lanes map[string]*Lane, aliasLabel string) string {
	for name, lane := range lanes {
		for _, r := range lane.Ranks {
			if r.Rank == 0 && r.AliasValue != "" {
				return name
			}
		}
	}
	return ""
}

// ExistingAliasValue returns the alias value currently in use anywhere in the
// namespace (falls back to the default when nothing holds it).
func ExistingAliasValue(lanes map[string]*Lane, aliasLabel string) string {
	for _, lane := range lanes {
		for _, r := range lane.Ranks {
			if r.AliasValue != "" {
				return r.AliasValue
			}
		}
	}
	return DefaultAliasValue
}

// --- waits ---

// waitRunning blocks until the deployment has at least one pod with every
// container Running. Works in stages where readiness probes do not exist yet.
func waitRunning(k *Kube, ns, deploy string, timeout time.Duration, out io.Writer) error {
	if k.dryRun {
		k.Plan = append(k.Plan, fmt.Sprintf("(wait until a pod of %s/%s is Running, timeout %s)", ns, deploy, timeout))
		return nil
	}
	deadline := time.Now().Add(timeout)
	lastReason := ""
	for {
		owners, err := k.DeploymentOwners(ns)
		if err == nil {
			pods, perr := k.ListPods(ns)
			if perr == nil {
				running := false
				for _, p := range pods {
					if owners[p.Metadata.Name] != deploy || p.IsTerminating() {
						continue
					}
					if p.AllContainersRunning() {
						running = true
						break
					}
					if r := p.WorstWaitingReason(); r != "" {
						lastReason = r
					}
				}
				if running {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			if lastReason != "" {
				return fmt.Errorf("timed out waiting for a Running pod of %s/%s (last container reason: %s)", ns, deploy, lastReason)
			}
			return fmt.Errorf("timed out waiting for a Running pod of %s/%s after %s", ns, deploy, timeout)
		}
		time.Sleep(5 * time.Second)
	}
}

// waitGone blocks until the deployment has zero pods, force-deleting pods
// stuck in Terminating along the way.
func waitGone(k *Kube, ns, deploy string, timeout time.Duration, out io.Writer) error {
	if k.dryRun {
		k.Plan = append(k.Plan, fmt.Sprintf("(wait until pods of %s/%s are gone, timeout %s)", ns, deploy, timeout))
		return nil
	}
	deadline := time.Now().Add(timeout)
	for {
		owners, err := k.DeploymentOwners(ns)
		if err == nil {
			pods, perr := k.ListPods(ns)
			if perr == nil {
				remaining := 0
				for _, p := range pods {
					if owners[p.Metadata.Name] != deploy {
						continue
					}
					remaining++
					if p.IsTerminating() {
						o_logf(out, "pod %s stuck Terminating; force-deleting", p.Metadata.Name)
						if err := k.DeletePod(ns, p.Metadata.Name, true); err != nil {
							return err
						}
					}
				}
				if remaining == 0 {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out: pods of %s/%s still present after %s", ns, deploy, timeout)
		}
		time.Sleep(5 * time.Second)
	}
}

func o_logf(out io.Writer, format string, args ...any) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "    "+format+"\n", args...)
}

// --- verbs ---

// Deploy brings a lane up in rank order (worker ranks first, head last) and
// waits for each rank's pod to run; the head additionally waits for Ready.
// It refuses to start while a different lane already serves the alias.
func Deploy(k *Kube, lanes map[string]*Lane, laneName string, o Options) error {
	o.Defaults()
	lane, err := GetLane(lanes, laneName)
	if err != nil {
		return err
	}
	if resident := AliasHolder(lanes, o.AliasLabel); resident != "" && resident != laneName {
		// Allowed only when the resident lane is fully scaled down (a switch in
		// progress); otherwise the Sparks are exclusive — refuse and point at switch.
		if r := residentActive(lanes[resident]); r {
			return fmt.Errorf("lane %q currently holds alias %q and is active — use `lane switch %s` (or drop it first)",
				resident, o.AliasLabel, laneName)
		}
		o.logf("lane %q holds the alias but is scaled to zero; allowing deploy of %s", resident, laneName)
	}
	for _, r := range lane.UpOrder() {
		head := r.Rank == 0
		if r.Replicas >= 1 && r.Ready >= 1 {
			o.logf("%s: rank %d (%s) already Ready, skipping", laneName, r.Rank, r.Name)
			continue
		}
		o.logf("%s: rank %d (%s) -> replicas=1, waiting for Running", laneName, r.Rank, r.Name)
		if err := k.Scale(o.Namespace, r.Name, 1); err != nil {
			return err
		}
		if err := waitRunning(k, o.Namespace, r.Name, o.PerRankWait, o.Out); err != nil {
			return err
		}
		if head {
			o.logf("%s: head running; waiting for Ready (readiness /health)", laneName)
			if err := k.WaitReady(o.Namespace, r.Name, o.PerRankWait); err != nil {
				return err
			}
		}
		o.logf("%s: rank %d up", laneName, r.Rank)
	}
	o.logf("%s: deployed", laneName)
	return nil
}

// Drop shuts a lane down in head-first order, clearing stuck terminating pods.
func Drop(k *Kube, lanes map[string]*Lane, laneName string, o Options) error {
	o.Defaults()
	lane, err := GetLane(lanes, laneName)
	if err != nil {
		return err
	}
	for _, r := range lane.DownOrder() {
		if r.Replicas == 0 && r.Current == 0 {
			o.logf("%s: rank %d (%s) already at zero", laneName, r.Rank, r.Name)
			continue
		}
		o.logf("%s: rank %d (%s) -> replicas=0, waiting for pods gone", laneName, r.Rank, r.Name)
		if err := k.Scale(o.Namespace, r.Name, 0); err != nil {
			return err
		}
		if err := waitGone(k, o.Namespace, r.Name, 5*time.Minute, o.Out); err != nil {
			return err
		}
	}
	o.logf("%s: dropped", laneName)
	return nil
}

// MoveAlias points the alias label at the target lane's head (removing it
// from any other deployment that holds it).
func MoveAlias(k *Kube, lanes map[string]*Lane, target string, o Options) error {
	o.Defaults()
	tlane, err := GetLane(lanes, target)
	if err != nil {
		return err
	}
	thead := tlane.Head()
	if thead == nil {
		return fmt.Errorf("lane %q has no rank-0 (head) deployment", target)
	}
	value := ExistingAliasValue(lanes, o.AliasLabel)
	for _, lane := range lanes {
		for _, r := range lane.Ranks {
			holder := r.AliasValue != ""
			want := r.Name == thead.Name
			switch {
			case want && !holder:
				if err := k.PatchTemplateLabels(o.Namespace, r.Name, map[string]*string{o.AliasLabel: &value}); err != nil {
					return err
				}
				o.logf("alias %s=%s moved to %s", o.AliasLabel, value, r.Name)
			case !want && holder:
				if err := k.PatchTemplateLabels(o.Namespace, r.Name, map[string]*string{o.AliasLabel: nil}); err != nil {
					return err
				}
				o.logf("alias removed from %s", r.Name)
			}
		}
	}
	return nil
}

// Switch performs the full resident change: drop the current holder of the
// alias (or --from), deploy the target, move the alias, verify one endpoint.
func Switch(k *Kube, lanes map[string]*Lane, from, to string, o Options) error {
	o.Defaults()
	if _, err := GetLane(lanes, to); err != nil {
		return err
	}
	if from == "" {
		from = AliasHolder(lanes, o.AliasLabel)
		if from == "" {
			return fmt.Errorf("no lane currently holds alias %q; pass --from or use `lane deploy %s`", o.AliasLabel, to)
		}
	}
	if from == to {
		return fmt.Errorf("lane %q is already the resident", to)
	}
	o.logf("switch: %s -> %s (drop head-first, deploy worker-first, move alias, verify)", from, to)
	if err := Drop(k, lanes, from, o); err != nil {
		return fmt.Errorf("dropping %s: %w", from, err)
	}
	// Refresh state: the drop changed replicas, and Deploy's guard/skip logic
	// must not act on the stale snapshot.
	fresh, derr := Discover(k, o.Namespace, o.AliasLabel)
	if derr != nil {
		return fmt.Errorf("refreshing lane state: %w", derr)
	}
	lanes = fresh
	if err := Deploy(k, lanes, to, o); err != nil {
		return fmt.Errorf("deploying %s: %w", to, err)
	}
	if err := MoveAlias(k, lanes, to, o); err != nil {
		return fmt.Errorf("moving alias: %w", err)
	}
	eps, err := k.Endpoints(o.Namespace, o.StableService)
	if err != nil {
		o.logf("could not verify stable service %s: %v", o.StableService, err)
		return nil
	}
	if len(eps) != 1 {
		o.logf("WARNING: stable service %s has %d endpoints (%s); expected exactly 1 — check `kubectl get endpoints %s -n %s`",
			o.StableService, len(eps), strings.Join(eps, ", "), o.StableService, o.Namespace)
		return nil
	}
	o.logf("stable service %s endpoints: %v", o.StableService, eps)
	return nil
}

// Assessment is the pure outcome of inspecting a lane (unit-testable core).
type Assessment struct {
	Lane         string
	ForceDeletes []string // pods to force-delete (stuck terminating or on dead nodes)
	Recreate     bool     // lane needs coordinated recreation
	Reason       string
	Healthy      bool
}

// recReasons are container wait states that justify coordinated recreation.
func recReason(reason string) bool {
	switch reason {
	case "CrashLoopBackOff", "Error", "CreateContainerError", "RunContainerError",
		"ImagePullBackOff", "ErrImagePull", "InvalidImageName", "OOMKilled":
		return true
	}
	return strings.HasPrefix(reason, "terminated:")
}

// AssessLane inspects pods/owners/nodes for one lane. Pure function.
func AssessLane(lane *Lane, pods []Pod, owners map[string]string, nodesReady map[string]bool) Assessment {
	a := Assessment{Lane: lane.Name, Healthy: true}
	laneDeps := map[string]bool{}
	for _, r := range lane.Ranks {
		laneDeps[r.Name] = true
	}
	anyRec := false
	for _, p := range pods {
		dep := owners[p.Metadata.Name]
		if !laneDeps[dep] {
			continue
		}
		if p.IsTerminating() {
			a.ForceDeletes = append(a.ForceDeletes, p.Metadata.Name)
		} else if p.Spec.NodeName != "" && !nodesReady[p.Spec.NodeName] {
			// Pod placed on a node that is gone/NotReady — kubelet can never
			// confirm shutdown, so clear it. An unscheduled Pending pod (no
			// nodeName) is NOT stuck; leave it to the scheduler.
			a.ForceDeletes = append(a.ForceDeletes, p.Metadata.Name)
		}
		if r := p.WorstWaitingReason(); recReason(r) {
			anyRec = true
			a.Reason = fmt.Sprintf("pod %s: %s", p.Metadata.Name, r)
		}
		if p.Status.Phase == "Failed" {
			anyRec = true
			a.Reason = fmt.Sprintf("pod %s: phase Failed", p.Metadata.Name)
		}
	}
	// A rank with desired replicas but zero pods at all is orphaned (e.g. the
	// only pod was evicted and never rescheduled) — recreate the pair.
	for _, r := range lane.Ranks {
		if r.Replicas > 0 && r.Current == 0 {
			anyRec = true
			a.Reason = fmt.Sprintf("rank %s: spec.replicas=%d but no pods exist", r.Name, r.Replicas)
		}
	}
	if anyRec {
		a.Recreate = true
		a.Healthy = false
	}
	if len(a.ForceDeletes) > 0 {
		a.Healthy = false
	}
	return a
}

// Reconcile heals lanes: force-clears stuck/off-node pods and recreates any
// lane whose ranks are crash-looping or orphaned (coordinated pair recreation
// per RISK-0018). With no names, all lanes in the namespace are assessed.
func Reconcile(k *Kube, lanes map[string]*Lane, names []string, pods []Pod, owners map[string]string, nodesReady map[string]bool, o Options) ([]Assessment, error) {
	o.Defaults()
	if len(names) == 0 {
		for n := range lanes {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	results := []Assessment{}
	for _, name := range names {
		lane, err := GetLane(lanes, name)
		if err != nil {
			return results, err
		}
		a := AssessLane(lane, pods, owners, nodesReady)
		_ = lane // GetLane validated existence; actions re-resolve below
		results = append(results, a)
		switch {
		case a.Recreate:
			o.logf("%s: recreating lane (%s)", name, a.Reason)
			if err := Drop(k, lanes, name, o); err != nil {
				return results, fmt.Errorf("recreate %s: %w", name, err)
			}
			// Drop changed every rank's desired/current state. Rediscover before
			// Deploy so its healthy-rank skip logic cannot act on the stale
			// pre-drop snapshot and leave the surviving rank scaled to zero.
			fresh, derr := Discover(k, o.Namespace, o.AliasLabel)
			if derr != nil {
				return results, fmt.Errorf("refreshing lane state for %s: %w", name, derr)
			}
			lanes = fresh
			if err := Deploy(k, lanes, name, o); err != nil {
				return results, fmt.Errorf("recreate %s: %w", name, err)
			}
		case len(a.ForceDeletes) > 0:
			for _, pod := range a.ForceDeletes {
				o.logf("%s: force-deleting pod %s", name, pod)
				if err := k.DeletePod(o.Namespace, pod, true); err != nil {
					return results, err
				}
			}
		default:
			o.logf("%s: healthy", name)
		}
	}
	return results, nil
}
