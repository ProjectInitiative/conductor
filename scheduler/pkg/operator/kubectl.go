// Package operator implements the lane lifecycle operator: it discovers model
// lanes from cluster labels and deploys/switches/drops/reconciles them in the
// rank order the TP runbooks require (worker ranks first up, head first down,
// coordinated pair recreation on rank loss).
//
// It shells out to kubectl so it inherits the operator's kubeconfig, context,
// and RBAC both in-cluster and from a workstation. Every mutating call is
// guarded: without --yes the operator only prints the plan.
package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Runner executes a kubectl argv and returns stdout.
type Runner interface {
	Run(ctx context.Context, argv []string) ([]byte, error)
}

// ExecRunner shells out to kubectl (default binary "kubectl").
type ExecRunner struct{ Binary string }

func (e ExecRunner) Run(ctx context.Context, argv []string) ([]byte, error) {
	bin := e.Binary
	if bin == "" {
		bin = "kubectl"
	}
	cmd := exec.CommandContext(ctx, bin, argv...)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("kubectl %s: %w", strings.Join(argv, " "), err)
	}
	return out, nil
}

// Kube is the kubectl-backed cluster handle. Read calls always execute;
// mutating calls are recorded into Plan and skipped when dryRun is set.
type Kube struct {
	Context string
	Runner  Runner
	dryRun  bool
	Plan    []string
}

// New builds a Kube handle. When dry is true, mutations are planned, not run.
func New(kubeContext string, dry bool) *Kube {
	return &Kube{Context: kubeContext, Runner: ExecRunner{}, dryRun: dry}
}

func (k *Kube) argv(args ...string) []string {
	av := []string{}
	if k.Context != "" {
		av = append(av, "--context", k.Context)
	}
	return append(av, args...)
}

func (k *Kube) read(args ...string) ([]byte, error) {
	return k.Runner.Run(context.Background(), k.argv(args...))
}

// act runs a mutating command, or records it in the plan when dry-running.
func (k *Kube) act(desc string, args ...string) error {
	if k.dryRun {
		k.Plan = append(k.Plan, fmt.Sprintf("%s\n      $ kubectl %s", desc, strings.Join(k.argv(args...), " ")))
		return nil
	}
	_, err := k.Runner.Run(context.Background(), k.argv(args...))
	return err
}

// --- cluster object types (minimal JSON projections) ---

type OwnerRef struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type Deployment struct {
	Metadata struct {
		Name      string            `json:"name"`
		Labels    map[string]string `json:"labels"`
		Namespace string            `json:"namespace"`
	} `json:"metadata"`
	Spec struct {
		Replicas int32 `json:"replicas"`
		Template struct {
			Metadata struct {
				Labels map[string]string `json:"labels"`
			} `json:"metadata"`
			Spec struct {
				Containers []struct {
					Name string   `json:"name"`
					Args []string `json:"args"`
				} `json:"containers"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
	Status struct {
		Replicas          int32 `json:"replicas"`
		ReadyReplicas     int32 `json:"readyReplicas"`
		AvailableReplicas int32 `json:"availableReplicas"`
	} `json:"status"`
}

func (d *Deployment) RankArgs() []string {
	var args []string
	for _, c := range d.Spec.Template.Spec.Containers {
		args = append(args, c.Args...)
	}
	return args
}

type ContainerState struct {
	Running *struct{} `json:"running"`
	Waiting *struct {
		Reason string `json:"reason"`
	} `json:"waiting"`
	Terminated *struct {
		ExitCode int32  `json:"exitCode"`
		Reason   string `json:"reason"`
	} `json:"terminated"`
}

type Pod struct {
	Metadata struct {
		Name              string            `json:"name"`
		Namespace         string            `json:"namespace"`
		DeletionTimestamp *string           `json:"deletionTimestamp"`
		Labels            map[string]string `json:"labels"`
		OwnerReferences   []OwnerRef        `json:"ownerReferences"`
	} `json:"metadata"`
	Spec struct {
		NodeName string `json:"nodeName"`
	} `json:"spec"`
	Status struct {
		Phase             string `json:"phase"`
		ContainerStatuses []struct {
			Name  string         `json:"name"`
			State ContainerState `json:"state"`
		} `json:"containerStatuses"`
	} `json:"status"`
}

func (p *Pod) IsTerminating() bool { return p.Metadata.DeletionTimestamp != nil }

// AllContainersRunning reports whether every declared container is Running.
func (p *Pod) AllContainersRunning() bool {
	if len(p.Status.ContainerStatuses) == 0 {
		return false
	}
	for _, cs := range p.Status.ContainerStatuses {
		if cs.State.Running == nil {
			return false
		}
	}
	return true
}

// WorstWaitingReason surfaces CrashLoopBackOff/Error-style failures.
func (p *Pod) WorstWaitingReason() string {
	reasons := []string{}
	for _, cs := range p.Status.ContainerStatuses {
		if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" {
			reasons = append(reasons, cs.State.Waiting.Reason)
		}
		if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
			reasons = append(reasons, "terminated:"+cs.State.Terminated.Reason)
		}
	}
	if len(reasons) == 0 {
		return ""
	}
	sort.Strings(reasons)
	return reasons[len(reasons)-1]
}

type nodeList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Status struct {
			Conditions []struct {
				Type   string `json:"type"`
				Status string `json:"status"`
			} `json:"conditions"`
		} `json:"status"`
	} `json:"items"`
}

// --- reads ---

func (k *Kube) ListDeployments(ns string) ([]Deployment, error) {
	out, err := k.read("get", "deployments", "-n", ns, "-o", "json")
	if err != nil {
		return nil, err
	}
	var list struct {
		Items []Deployment `json:"items"`
	}
	if err := json.Unmarshal(out, &list); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (k *Kube) GetDeployment(ns, name string) (*Deployment, error) {
	out, err := k.read("get", "deployment", name, "-n", ns, "-o", "json")
	if err != nil {
		return nil, err
	}
	var d Deployment
	if err := json.Unmarshal(out, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

func (k *Kube) ListPods(ns string) ([]Pod, error) {
	out, err := k.read("get", "pods", "-n", ns, "-o", "json")
	if err != nil {
		return nil, err
	}
	var list struct {
		Items []Pod `json:"items"`
	}
	if err := json.Unmarshal(out, &list); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// DeploymentOwners returns pod-name -> owning Deployment name, resolved
// through the ReplicaSet layer (works regardless of labels).
func (k *Kube) DeploymentOwners(ns string) (map[string]string, error) {
	out, err := k.read("get", "replicasets", "-n", ns, "-o", "json")
	if err != nil {
		return nil, err
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Name            string     `json:"name"`
				OwnerReferences []OwnerRef `json:"ownerReferences"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(out, &list); err != nil {
		return nil, err
	}
	rsToDeploy := map[string]string{}
	for _, rs := range list.Items {
		for _, ref := range rs.Metadata.OwnerReferences {
			if ref.Kind == "Deployment" {
				rsToDeploy[rs.Metadata.Name] = ref.Name
			}
		}
	}
	pods, err := k.ListPods(ns)
	if err != nil {
		return nil, err
	}
	m := map[string]string{}
	for _, p := range pods {
		for _, ref := range p.Metadata.OwnerReferences {
			if ref.Kind == "ReplicaSet" {
				if dep, ok := rsToDeploy[ref.Name]; ok {
					m[p.Metadata.Name] = dep
				}
			}
		}
	}
	return m, nil
}

// NodeReady returns node name -> Ready condition true/false.
func (k *Kube) NodeReady() (map[string]bool, error) {
	out, err := k.read("get", "nodes", "-o", "json")
	if err != nil {
		return nil, err
	}
	var list nodeList
	if err := json.Unmarshal(out, &list); err != nil {
		return nil, err
	}
	m := map[string]bool{}
	for _, n := range list.Items {
		ready := false
		for _, c := range n.Status.Conditions {
			if c.Type == "Ready" {
				ready = c.Status == "True"
			}
		}
		m[n.Metadata.Name] = ready
	}
	return m, nil
}

// Endpoints returns the "ip:port" endpoints behind a Service in a namespace.
// kubectl returns a bare Endpoints object for a named get and a list for a
// label-selector get; both shapes are handled.
func (k *Kube) Endpoints(ns, svc string) ([]string, error) {
	out, err := k.read("get", "endpoints", svc, "-n", ns, "-o", "json")
	if err != nil {
		return nil, err
	}
	type subset struct {
		Addresses []struct {
			IP string `json:"ip"`
		} `json:"addresses"`
		Ports []struct {
			Port int `json:"port"`
		} `json:"ports"`
	}
	var eps []string
	collect := func(subsets []subset) {
		for _, s := range subsets {
			for _, a := range s.Addresses {
				for _, p := range s.Ports {
					eps = append(eps, fmt.Sprintf("%s:%d", a.IP, p.Port))
				}
			}
		}
	}
	var single struct {
		Subsets []subset `json:"subsets"`
	}
	if err := json.Unmarshal(out, &single); err == nil && len(single.Subsets) > 0 {
		collect(single.Subsets)
		return eps, nil
	}
	var list struct {
		Items []struct {
			Subsets []subset `json:"subsets"`
		} `json:"items"`
	}
	if err := json.Unmarshal(out, &list); err != nil {
		return nil, err
	}
	for _, item := range list.Items {
		collect(item.Subsets)
	}
	sort.Strings(eps)
	return eps, nil
}

// WaitReady blocks until the deployment reports at least one ready/available
// replica (readiness probe gate).
func (k *Kube) WaitReady(ns, name string, timeout time.Duration) error {
	if k.dryRun {
		k.Plan = append(k.Plan, fmt.Sprintf("(wait until %s/%s is Ready, timeout %s)", ns, name, timeout))
		return nil
	}
	deadline := time.Now().Add(timeout)
	for {
		var d struct {
			Status struct {
				ReadyReplicas     int32 `json:"readyReplicas"`
				AvailableReplicas int32 `json:"availableReplicas"`
			} `json:"status"`
		}
		out, err := k.read("get", "deployment", name, "-n", ns, "-o", "json")
		if err == nil && json.Unmarshal(out, &d) == nil {
			if d.Status.ReadyReplicas >= 1 || d.Status.AvailableReplicas >= 1 {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s/%s to become Ready after %s", ns, name, timeout)
		}
		time.Sleep(5 * time.Second)
	}
}

// --- mutations (planned when dry) ---

func (k *Kube) Scale(ns, name string, replicas int32) error {
	return k.act(
		fmt.Sprintf("scale deployment %s/%s to replicas=%d", ns, name, replicas),
		"scale", "deployment", name, "-n", ns, "--replicas", strconv.Itoa(int(replicas)),
	)
}

func (k *Kube) DeletePod(ns, name string, force bool) error {
	args := []string{"delete", "pod", name, "-n", ns}
	desc := fmt.Sprintf("delete pod %s/%s", ns, name)
	if force {
		args = append(args, "--grace-period=0", "--force")
		desc += " (grace-period=0 --force)"
	}
	return k.act(desc, args...)
}

// PatchTemplateLabels merge-patches a Deployment's pod template labels.
// A nil value removes the label key.
func (k *Kube) PatchTemplateLabels(ns, name string, labels map[string]*string) error {
	if len(labels) == 0 {
		return nil
	}
	body := map[string]any{
		"spec": map[string]any{
			"template": map[string]any{
				"metadata": map[string]any{
					"labels": labels,
				},
			},
		},
	}
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	parts := []string{}
	for key, v := range labels {
		if v == nil {
			parts = append(parts, fmt.Sprintf("%s- (removed)", key))
		} else {
			parts = append(parts, fmt.Sprintf("%s=%s", key, *v))
		}
	}
	sort.Strings(parts)
	return k.act(
		fmt.Sprintf("patch deployment %s/%s pod-template labels: %s", ns, name, strings.Join(parts, ", ")),
		"patch", "deployment", name, "-n", ns, "--type=merge", "-p", string(b),
	)
}

// --- single-flight lock (ConfigMap lease) ---

const LockConfigMap = "conductor-lane-lock"

// AcquireLock takes the lane-operator lease in a namespace. It returns a
// release function. A fresh foreign lease blocks; --force steals it. Leases
// older than 30 minutes are considered stale and stolen automatically.
func (k *Kube) AcquireLock(ns, owner, verb string, force bool) (func(), error) {
	since := time.Now().UTC().Format(time.RFC3339)
	err := k.act(
		fmt.Sprintf("acquire lane lock %s/%s (owner=%s verb=%s)", ns, LockConfigMap, owner, verb),
		"create", "configmap", LockConfigMap, "-n", ns,
		"--from-literal=owner="+owner,
		"--from-literal=verb="+verb,
		"--from-literal=since="+since,
	)
	if err == nil {
		return k.releaseLock(ns), nil
	}
	// Exists (or failed): inspect the lease.
	out, rerr := k.read("get", "configmap", LockConfigMap, "-n", ns, "-o", "json")
	if rerr != nil {
		return nil, fmt.Errorf("lane lock unavailable: %v (create error: %v)", rerr, err)
	}
	var cm struct {
		Data map[string]string `json:"data"`
	}
	if jerr := json.Unmarshal(out, &cm); jerr != nil {
		return nil, fmt.Errorf("lane lock unreadable: %w", jerr)
	}
	if !force {
		if s, ok := cm.Data["since"]; ok {
			if t, perr := time.Parse(time.RFC3339, s); perr == nil {
				if time.Since(t) < 30*time.Minute {
					return nil, fmt.Errorf(
						"another lane operation is in flight (owner=%s verb=%s since=%s); use --force to steal the lease",
						cm.Data["owner"], cm.Data["verb"], s)
				}
			}
		}
	}
	if err := k.act(
		fmt.Sprintf("steal lane lock %s/%s (owner=%s)", ns, LockConfigMap, owner),
		"patch", "configmap", LockConfigMap, "-n", ns,
		"-p", fmt.Sprintf(`{"data":{"owner":%q,"verb":%q,"since":%q}}`, owner, verb, since),
	); err != nil {
		return nil, err
	}
	return k.releaseLock(ns), nil
}

func (k *Kube) releaseLock(ns string) func() {
	return func() {
		_ = k.act("release lane lock", "delete", "configmap", LockConfigMap, "-n", ns, "--ignore-not-found=true")
	}
}
