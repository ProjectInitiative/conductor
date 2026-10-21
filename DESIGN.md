# Conductor operator design

This document is the implementation contract for Conductor's Kubernetes-native
control plane. It supersedes the original Fission/kubectl swap design.

## 1. Principles

1. **Desired state, not commands.** GitOps writes permanent definitions;
   temporary users and the future UI write expiring overrides.
2. **Observed state is separate.** Status and Conditions explain what exists,
   what Conductor intends, why, and which transition is active.
3. **One bounded action per reconcile.** No 45-minute blocking waits.
4. **Explicit adoption only.** Conductor never manages a Deployment because its
   name resembles a lane. A `ManagedWorkload` must reference it.
5. **Safety outranks availability.** Node partition without fencing blocks
   relocation instead of risking duplicate hostNetwork/GPU processes.
6. **GitOps owns templates; Conductor owns lifecycle.** Initially the operator
   references existing Deployments and mutates only `/scale`. Future
   operator-created children require explicit typed drivers and owner inventory.
7. **Architecture neutral.** A static Go binary is published as an amd64+arm64
   OCI index and runs without kubectl or a shell.

## 2. API group

Milestone one uses `conductor.projectinitiative.io/v1alpha1`. The version is
alpha intentionally; API shapes may evolve while the first non-LLM driver is
qualified.

### 2.1 ResourcePool

A pool is an arbitration and failure domain. `dgx-spark` has one exclusive slot;
future pools may represent astrolabe interactive GPU/CPU/memory claims.

Important fields:

- `managementMode: Observe | Manage`
- capacity (one slot today)
- eligible node selector
- stable routing Service and selector
- NotReady grace, fencing requirement, dwell, and cooldown policy

Status will carry active/effective workload, the current operation, and standard
Conditions.

### 2.2 ManagedWorkload

A permanent Git-managed entry. The initial `DeploymentSet` driver references
existing Deployments with explicit role, replicas, start order, stop order, and
ready gate. `RecreateTogether` expresses the TensorFold failure domain.

Status is the operator/UI read model:

- `observedGeneration`
- effective intent and source
- phase and persisted operation journal
- member generation/node/replica/readiness summaries
- ready endpoint count
- `Ready`, `Progressing`, `Degraded`, and `Blocked` Conditions

Arbitrary embedded Kubernetes manifests are deliberately excluded from
v1alpha1. Generic recipes later reference typed drivers or versioned OCI bundles
with explicit inventory and ownership semantics.

### 2.2.1 ModelDownload and DownloaderClass

`ModelDownload` is the typed cache-transfer driver. It accepts only pinned
repository/revision entries, explicit Ensure/Delete intent, a PVC and optional
node selector, worker counts, verification receipt path, and an aggregate
`rateLimit.bytesPerSecond`. It creates exactly one owned Job and exposes its
phase, progress, throughput, and receipt in status. The controller performs
bounded create/observe/status actions; it never applies arbitrary pod or Job
manifests.

`DownloaderClass` is a cluster-scoped approved executor definition containing
the immutable downloader image reference, pull policy, service account, and
metrics port. This keeps image selection out of each request without making the
operator a general image or manifest launcher. The first class wraps the
existing rate-limited Python downloader; its shared token-bucket limiter remains
the authority for aggregate throughput across concurrent file workers.

### 2.3 WorkloadOverride

The only write surface for temporary CLI/web intent. It is separate from the
GitOps-owned workload so Argo cannot revert an interactive decision.

Modes:

- `Pin` — make one workload effective in its pool;
- `Suspend` — stop a target workload;
- `FreezePool` — preserve current placement while suppressing automation;
- `Automatic` — relinquish manual control.

Every override has optional expiry plus actor, reason, and idempotency key.
Precedence is:

```text
safety block > unexpired explicit override > permanent baseline > automatic demand
```

An override never bypasses lifecycle ordering, qualification, or fencing.

## 3. Controller process

`conductor controller` runs under controller-runtime with:

- two replicas and Lease leader election;
- shared cached reads plus uncached APIReader checks before destructive actions;
- watches for ManagedWorkloads, ResourcePools, Overrides, referenced
  Deployments/Pods, EndpointSlices, and Nodes;
- one concurrent reconcile initially because the DGX workloads share one pool;
- `/healthz`, `/readyz`, Prometheus controller metrics, structured logs, and
  Kubernetes Events;
- graceful cancellation through `SetupSignalHandler`.

The controller uses typed Kubernetes clients. The ServiceAccount projection
provides its token and API CA. It does not require kubectl, a shell, or a public
CA bundle.

## 4. DeploymentSet state machine

The operation phase is persisted in `ManagedWorkload.status`:

```text
Observing / Ready
  -> StoppingHead
  -> WaitingHeadGone
  -> StoppingWorkers
  -> WaitingAllGone
  -> StartingWorkers
  -> WaitingWorkerReady
  -> StartingHead
  -> Qualifying
  -> Ready
```

A reconcile re-reads the CR and live children, performs at most one idempotent
mutation, updates status, then requeues. Scaling uses the Deployment `/scale`
subresource; Conductor never applies a GET-derived Deployment.

Old Failed pods from obsolete ReplicaSets do not poison a healthy current
Deployment generation. Before a scale/delete/routing action, the controller must
re-read safety-critical objects through the uncached reader and honor UID and
resourceVersion changes.

## 5. Routing

The stable identity remains `dgx-spark-backend`. Recovery currently verifies its
EndpointSlices and does not change its selector.

A later switching milestone will patch only the stable Service selector after
direct qualification. It must not move a pod-template alias label: changing a
live head's template causes a one-rank rollout and violates the coordinated TP
contract.

## 6. Node loss and fencing

Leader election prevents two controller instances from mutating concurrently;
it does not fence a partitioned machine. A pod on a NotReady or absent node may
still own host ports, GPU memory, and RDMA processes.

Default behavior is therefore `Blocked/NodeNeedsFencing`. Future automatic
failover requires a `Fencer` interface (for example BMC/power control or another
provable node shutdown mechanism). Only a successful fencing receipt may permit
force deletion and relocation. The web UI must make this distinction explicit.

## 7. Ownership and migration

- Homelab continues to own TensorFold Deployment templates.
- Conductor's generator installs only CRDs, RBAC, controller Deployment/PDB,
  and baseline CRs.
- The stale vLLM model templates, router, and watch triggers were never
  deployed and have been deleted; existing lanes remain homelab-owned.
- `dgx-spark` begins in `Observe`; only `tensorfold-v14` is an initial adoption
  candidate.
- Argo must ignore adopted Deployment replica fields before `Manage` is enabled
  if those Deployments later become GitOps-managed.
- The kubectl lane CLI is transitional break-glass tooling. Its stale-snapshot
  bug is fixed, but autonomous control uses only the CR-backed reconciler.

## 8. Extension seams

The operator is intentionally split around three concepts:

- **Driver:** observe, activate/deactivate one step, qualify, route, clean up.
- **Planner:** pools + workloads + demands + overrides -> effective assignment.
- **API/status:** UI reads status and writes overrides/demands only.

This supports future LLM, build, gaming, and other permanent-temporary workloads
without turning the first recovery controller into a general manifest engine.

## 9. Required validation before Manage

1. Unit tests for intent precedence, ordering, stale pods, endpoint cardinality,
   and restart at every phase.
2. Generator tests proving no model/router/watch-trigger resources are emitted.
3. One OCI index containing amd64 and arm64 manifests.
4. Observe-mode status/event comparison against the live TensorFold lane.
5. Controlled pod failure, controller restart, and leader failover.
6. Controlled node failure with explicit fencing decision.
7. Direct health plus exactly one stable ready endpoint before and after recovery.
