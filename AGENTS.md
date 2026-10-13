# Conductor — Agent guidance

Conductor is a Nix/devenv project that combines a **cdk8s Python generator**
(`generator/`) with a **Go operator binary** (`scheduler/`). The runtime is a
Kubernetes-native controller-runtime operator plus a break-glass lane CLI.
`DESIGN.md` is the contract and `ROADMAP.md` defines future workload/UI
expansion.

**The cdk8s generator renders the deployable operator control plane** (CRDs,
RBAC, controller Deployment/PDB, and baseline custom resources). Existing model
Deployments remain homelab-owned and are referenced explicitly. There is **no
hand-written Kustomize** — do not add a `manifests/` tree.

## Layout (disjoint agent domains — edit only your own)

| Path | Owner | Notes |
|---|---|---|
| `generator/` | agent-A | cdk8s generator: emits deployable objects from apps.yaml + clusters/*.yaml |
| `scheduler/` | agent-B | Go: CRD APIs, operator, policy engine, migration CLI, legacy gateway |
| `apps.yaml`, `clusters/` | agent-A | source of truth (model profiles, namespace, image tag) |

Only edit files under your assigned path. The parent merges branches.

## Conventions

- **Nix / devenv**: `use flake` in `.envrc`; shell provided by `flake.nix` +
  `devenv.nix`. Tools: `generate-manifests`, `import-crds`, `go build/test`, `nix run .#conductor -- controller`.
- **Go**: module `github.com/projectinitiative/conductor`. Modes:
  `conductor controller` (operator) | `conductor lane <verb>` (break-glass).
  Keep reconciliation logic **pure & clock-injectable**; test with a fake
  clock / fake client (no cluster).
- **cdk8s**: emit resources via `ApiObject` + `JsonPatch.add` (raw YAML) so no
  pre-generated CRD stubs are required. `import-crds` is optional.
- **Formatting**: `nixfmt` for `.nix`, `gofmt` for Go (excluding `vendor/`),
  `ruff` for Python. The flake `checks.tests` runs `go test ./...`; `checks.formatting` checks nixfmt/gofmt.
- **Unit tests only.** No integration tests. Do not try to touch a real cluster.

## Definition of done per agent

1. Code compiles / generator runs.
2. Unit tests pass (`go test ./...` or `pytest`).
3. `gofmt`/`nixfmt` clean.
4. Commit on your worktree branch with a clear message.
