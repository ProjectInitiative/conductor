# Conductor

LLM model-serving **router** and **swap scheduler** for a 2× DGX Spark cluster.
Several OpenAI-compatible models (DeepSeek / GLM / Qwen) each use BOTH GB10 nodes,
so only one is resident at a time. Conductor routes requests by model name (via
LiteLLM) and schedules the (expensive) residency swap with a **priority +
traffic-light** policy so it never thrashes loading/unloading and lower-priority
models still get served.

## Components

- **`generator/`** — cdk8s generator that renders Argo CD `Application` CRDs from a
  declarative catalog (`apps.yaml` + `clusters/*.yaml`).
- **`scheduler/`** — Go multi-headed binary: the scheduler + traffic-light gateway
  (`conductor server`) and the Fission functions (`conductor function <op>`).
- **`manifests/`** — Kustomize bases for the router, scheduler deployment, fission
  resources, per-model templates, RBAC and services.

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
