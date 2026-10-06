# Conductor

LLM model-serving **router** and **swap scheduler** for a 2× DGX Spark cluster.
Several OpenAI-compatible models (DeepSeek / GLM / Qwen) each use BOTH GB10 nodes,
so only one is resident at a time. Conductor routes requests by model name (via
LiteLLM) and schedules the (expensive) residency swap with a **priority +
traffic-light** policy so it never thrashes loading/unloading and lower-priority
models still get served.

> **2026-10 update:** the resident serving engine is now TensorFold (v1.4 flash
> pair, 3× full-model lane staged), and routing follows the stable alias
> contract (homelab DEC-0026: the head whose pod template carries
> `serving.homelab.io/alias=dgx-spark` is resident; the `dgx-spark-backend`
> Service selects on it). The `lane` operator below is built directly on that
> contract today; the traffic-light scheduler will drive the same mechanism
> when it is deployed.

## Operator — lane lifecycle (`conductor lane`)

The `lane` verb set encodes the house runbooks (worker ranks first up, head
first down, coordinated pair recreation on rank loss, stuck-pod force-clears,
alias-label routing). Lanes are discovered from cluster labels — any Deployment
labeled `lane=<name>` or named `<name>-head`/`<name>-worker[1-9]` in the target
namespace. It shells out to `kubectl`, so it works from a workstation or
in-cluster with your existing kubeconfig/RBAC.

```bash
go build -o conductor ./scheduler/cmd/conductor    # or: nix run .#conductor

conductor lane status                    # lanes, ranks, resident, issues, stable endpoints
conductor lane deploy <lane> --yes       # stage → active (worker-first, waits Running/Ready)
conductor lane switch <to> --yes         # drop resident (head-first) → deploy target → move alias → verify
conductor lane switch <to> --dry-run     # preview the exact kubectl sequence, change nothing
conductor lane drop <lane> --yes         # active → staged
conductor lane alias <lane> --yes        # move the serving alias only
conductor lane reconcile --yes           # heal: force-clear stuck/off-node pods; recreate crash-looped pairs
conductor lane reconcile tensorfold-v14 --yes   # scope to one lane
```

Safety rails: every mutating verb **prints its plan and requires `--yes`**;
`--dry-run` shows the kubectl commands without touching anything; mutating runs
hold a single-flight lock (ConfigMap lease, `--force` to steal a stale one);
`deploy` refuses while another lane is still serving the alias — use `switch`.
After a switch, commit the alias-label move to the homelab manifests so a future
apply doesn't revert it.

Adding a brand-new lane is still a homelab manifest change (checked-in
Deployments at `replicas: 0`); `conductor lane` operates on lanes that exist in
the cluster — deploy/switch/drop/reconcile them.

## Components

- **`generator/`** — a **cdk8s** app. The single place that decides how the whole
  stack is built: it renders the *deployable* Kubernetes objects (Deployments,
  Services, ConfigMaps, Fission CRs, RBAC, per-model head/worker templates,
  Tailscale Service) from `apps.yaml` + `clusters/*.yaml`. No operator, no Helm
  chart, no hand-written Kustomize for the deployables.
- **`scheduler/`** — Go multi-headed binary: the scheduler + traffic-light gateway
  (`conductor server`) and the Fission functions (`conductor function <op>`).

## Deploying (homelab is purely a deployment mechanism)

Homelab adds **one** plain Argo CD `Application` whose `source` is this repo's
`generator`, run through the **`cdk8s-v1.0` CMP plugin** (already installed for
homelab-root). Argo CD "calls" the generator, which emits the self-contained
Conductor app, and applies it:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: conductor
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/projectinitiative/conductor.git
    targetRevision: HEAD
    path: generator
    plugin:
      name: cdk8s-v1.0
  destination:
    server: https://kubernetes.default.svc
    namespace: llm-test
  syncPolicy:
    automated: { prune: true, selfHeal: true }
```

Conductor owns all "how to build" logic (image built by its CI + the cdk8s
rendering). Homelab only registers the app.

See `DESIGN.md` for the full architecture and contracts.

## Development

```bash
nix develop            # or direnv allow
generate-manifests     # render Argo CD Applications to .direnv/manifests
import-crds            # regenerate cdk8s CRD stubs
cd scheduler && go test ./...   # policy engine tests
nix run .#conductor -- server   # local gateway (no cluster)
```

## Boundaries

Assumed cluster-provided (not owned here): Argo CD + cdk8s CMP, `juicefs-sc` +
HuggingFace cache, Fission + Python env + in-cluster k8s action, NVIDIA device
plugin, Tailscale `ai` LB, OpenBao/VSO for the HF token, node hostPath JIT caches.
