# Conductor

LLM model-serving **router** and **swap scheduler** for a 2× DGX Spark cluster.
Several OpenAI-compatible models (DeepSeek / GLM / Qwen) each use BOTH GB10 nodes,
so only one is resident at a time. Conductor routes requests by model name (via
LiteLLM) and schedules the (expensive) residency swap with a **priority +
traffic-light** policy so it never thrashes loading/unloading and lower-priority
models still get served.

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
