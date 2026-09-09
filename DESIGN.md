# Conductor — LLM model-serving router & swap scheduler for 2× DGX Spark

Conductor is the control/data plane in front of several **swappable** OpenAI-compatible
LLM deployments (DeepSeek / GLM / Qwen) that each use BOTH GB10 nodes (TP=2, hostNetwork,
fixed fabric IPs), so only **one model is resident at a time**. It:

1. Routes requests by model name (via LiteLLM), and
2. Schedules the (expensive) model residency swap with a **priority + traffic-light**
   policy so it never thrashes loading/unloading, and lower-priority models still
   get served.

This document is the single source of truth for the contracts agents build to.

---

## 1. Components & responsibilities

| Component | Where | Language | Role |
|---|---|---|---|
| cdk8s generator | `generator/` | Python | Renders Argo CD `Application` CRDs + component manifests from a declarative catalog |
| Scheduler service | `scheduler/` | Go | The swap orchestrator + traffic-light gateway. Custom-code entity. |
| Fission functions | `fission/` | Go (same binary) | Discrete, stateless surface ops (status, swap-request, kubewatcher ready handler) |
| Router | `router/` (manifests) | LiteLLM (Helm) | Front OpenAI-compatible gateway: auth/permissions, virtual model names, `/v1/models` |
| Per-model templates | `manifests/models/` | Kustomize | Head + worker Deployments (replicas:0) + model profiles |
| RBAC / services | `manifests/` | Kustomize | Namespaced ServiceAccount/Role, Tailscale `ai` Service, model Services |

### Request path

```
Tailscale `ai` ──► LiteLLM (auth, virtual model names, /v1/models)
                        │  maps model_name → conductor gateway for that model
                        ▼
                 Conductor gateway (traffic-light admission + stream-hold)
                        │  if resident & green → proxy to resident vLLM backend
                        │  else → park/hold (or 503) per policy
                        ▼
                 vLLM (172.16.4.55:<port>)  [resident model]
```

### Control path

```
Conductor scheduler:
  desired/arriving requests ─► per-model FIFO queues (Redis)
  traffic-light policy (priority + quantum + min-dwell + cooldown) ─► choose resident
  swap lifecycle: scale down old head+worker → scale up target worker→head → poll /health
  publish state ─► model-state ConfigMap + update LiteLLM config (hide evicted, show resident)
```

---

## 2. Contracts

### 2.1 Model profile (per model)

```yaml
model:
  name: deepseek-v4-flash-vision-exp     # served-model-name (OpenAI `model`)
  priority: 1                             # scheduler priority (lower = higher priority)
  image: ghcr.io/anemll/dspark-vllm-gx10:0.1.1  # informational
  head:
    deployment: deepseek-parity-head
    hostIP: 172.16.4.55
    port: 8000
  worker:
    deployment: deepseek-parity-worker
    hostIP: 172.16.4.56
  health:
    path: /health
    timeout: 3600s
  quantum:
    max_requests: 32
    max_seconds: 600
  min_dwell: 900s
  cooldown: 300s
```

### 2.2 Conductor gateway HTTP API

All OpenAI-compatible endpoints (see §3). Scheduler-only endpoints:

| Endpoint | Method | Purpose |
|---|---|---|
| `/v1/chat/completions`, `/v1/completions` | POST | Traffic-light admission + proxy (stream-hold) |
| `/v1/models` | GET | Only resident model (hidden others). |
| `/status` | GET | resident, queues, pending, last_swap. |
| `/models/<name>/load` | POST | Request residency for a model (enqueues priority). |
| `/health` | GET | Liveness/readiness of the gateway. |

### 2.3 Fission function surface

- `conductor function status` — same `/status` handler, exposed as a Fission function.
- `conductor function model-ready` — KubernetesWatch handler; called when a `*-head`
  Deployment becomes Ready; finalizes a swap (updates state + LiteLLM config).

### 2.4 Config / state

- **`conductor-state`** ConfigMap: `{resident, desired, status, started_at}` (single-flight).
- **`conductor-model-profiles`** ConfigMap: the per-model profile map above.
- **Redis** (or an in-memory queue in `-dev` mode): per-model FIFO queues + priorities.
- **LiteLLM config**: updated by the scheduler to mark evicted models `hidden: true`.

---

## 3. Go module layout (`scheduler/`)

Module `github.com/projectinitiative/conductor`, multi-headed binary pattern
(per `golden-template/compiled-serverless`):

```
scheduler/
├── cmd/conductor/main.go        # conductor [server | function status | function model-ready]
├── pkg/scheduler/               # POLICY ENGINE (pure, unit-testable)
│   ├── policy.go                # traffic-light / priority / quantum / dwell / cooldown
│   ├── queue.go                 # per-model FIFO queues
│   └── policy_test.go
├── pkg/gateway/                 # HTTP admission + stream proxy
│   ├── gateway.go
│   └── gateway_test.go
├── pkg/k8s/                     # scale down/up deployments, /health poll, single-flight
│   └── k8s.go
├── pkg/fission/                 # Fission function mode handlers
│   └── fission.go
└── go.mod / go.sum
```

Policy engine is the pure, heavily-unit-tested core. It must be **deterministic and
clock-injectable** so tests can drive simulated time (swap doesn't need real k8s).

---

## 4. cdk8s generator

Modeled on `homelab/generator`. Inputs:

- `apps.yaml` — catalog of deployable pieces (router, scheduler, fission, models, rbac, services).
- `clusters/<cluster>.yaml` — which pieces deploy to `mc`, overrides, patches.

Outputs Argo CD `Application` CRDs to `.direnv/manifests/`. For custom k8s pieces the
`Application` `sources[].path` points at this repo's own `manifests/<piece>` (git-path
source); `router` is a Helm source; `scheduler`/`fission` reference the built image
(injected as a Kustomize image override / configmap).

---

## 5. Testing strategy (blind dev)

- **Scheduler policy**: pure unit tests, deterministic clock. This is the crux — get the
  FIFO/priority/traffic-light/anti-thrash rules right here first.
- **cdk8s generator**: `pytest` that runs the generator and asserts the rendered YAML.
- **Gateway**: `httptest` against a stub backend; test admission decision + stream proxy.
- **No integration test** until the app is assembled in-cluster.

---

## 6. Boundaries (assumed cluster-provided, NOT owned by this repo)

Argo CD + cdk8s CMP · `juicefs-sc` + HuggingFace cache · Fission + Python env +
in-cluster k8s action · NVIDIA device plugin on chronometer/sextant · Tailscale
`ai` LB + `mc-ingress-proxy` · OpenBao/VSO for the HF token · node hostPath JIT caches.

This repo owns: the generator, the scheduler, the fission functions, the router config,
the per-model templates, and the namespaced RBAC.
