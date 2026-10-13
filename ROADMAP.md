# Conductor roadmap

## Milestone 1 — native recovery operator (current)

- `ResourcePool`, `ManagedWorkload`, and `WorkloadOverride` v1alpha1 APIs.
- controller-runtime manager, Lease leader election, status/Conditions, Events.
- reference-only `DeploymentSet` driver.
- persisted head-first down / worker-first up state machine.
- EndpointSlice qualification and conservative node-fencing block.
- control-plane-only cdk8s output.
- static multi-architecture controller image.
- `dgx-spark` + `tensorfold-v14` baseline in Observe mode.

Exit gate: controlled failure testing, reviewed Observe status, immutable image
digest, and explicit approval to set `managementMode: Manage`.

## Milestone 2 — pool assignment and safe lane switching

- Move stable routing from pod-template alias mutation to Service-selector
  switching after qualification.
- Make pool assignment authoritative and enforce one resident workload.
- Add idempotent switch phases, min-dwell/cooldown, rollback, and endpoint gate.
- Convert `conductor lane` commands to create overrides and wait on Conditions;
  retain an explicitly named unsafe direct mode only as break glass.
- Remove the legacy `pkg/k8s.Swapper` and Fission watch path.

## Milestone 3 — authenticated API and interactive dashboard

The dashboard is a view/controller client, not an alternate scheduler.

- Tailnet-authenticated web API and UI.
- Pool/workload health, member nodes, effective-intent source, current phase,
  pending plan, transition history, and Events.
- Pin, suspend, freeze, resume Automatic, TTL, actor, and required reason.
- Plan preview with safety/fencing warnings before override creation.
- Optimistic concurrency and idempotency keys for double-click/retry safety.
- Audit log derived from CR history and Kubernetes Events.

The UI writes only `WorkloadOverride` (and later aggregated demand); it never
patches Deployments directly.

## Milestone 4 — demand-aware priority scheduling

- Add aggregated `WorkloadDemand`: target, request count, oldest request,
  last-seen, and urgency. Never create one CR per inference request.
- Gateway emits low-churn demand; planner chooses among safe candidates.
- Priority, FIFO age, quantum, min-dwell, cooldown, and anti-thrash policy.
- Admission can hold/stream-wait or return an explicit retry response while a
  residency transition is in progress.
- Manual override continues to beat automatic demand.

## Milestone 5 — generic permanent-temporary recipes

Separate reusable **recipe** from instantiated **managed workload**.

- `WorkloadRecipe` references a versioned OCI artifact or typed driver config.
- Inventory and owner references for operator-created children.
- Lifecycle classes: `Resident`, `Standby`, and `Ephemeral`.
- Multi-dimensional pool claims: CPU, memory, GPU, named devices, ports,
  hostPath, and exclusive nodes.
- Drivers remain typed; no arbitrary unvalidated manifest blob in the API.

First non-LLM qualification: the `nix-steamos-container` gaming workload on
astrolabe's Strix Halo iGPU (`/dev/dri`, RADV/Mesa, Steam runtime,
Sunshine→Moonlight over tailnet). The planner should be able to suspend
astrolabe AI/build workloads, activate gaming, then return to the baseline.

## Milestone 6 — fencing and broader infrastructure arbitration

- `Fencer` interface with receipts and expiry.
- Integrate a proven power-control mechanism before automatic relocation from
  partitioned nodes.
- Optional pool-specific `ForceAfter` only when backed by fencing evidence.
- ARM compile capacity and other homelab workload recipes.
- Capacity-aware placement across overlapping pools and conflict claims.

## Non-goals until their milestone

- No arbitrary Kubernetes apply engine.
- No direct web mutations of Deployments.
- No automatic NotReady-node force deletion without fencing.
- No deployment of stale Conductor router/model/Fission resources.
- No `Manage` default before Observe-mode qualification.
