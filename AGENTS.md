# Conductor — Agent guidance

Conductor is a Nix/devenv project that combines a **cdk8s Python generator**
(`generator/`) with a **Go multi-headed binary** (`scheduler/`) that runs both
the swap scheduler/gateway and Fission functions. `DESIGN.md` is the contract.

## Layout (disjoint agent domains — edit only your own)

| Path | Owner | Notes |
|---|---|---|
| `generator/` | agent-A | cdk8s generator + catalog + cluster config + pytest |
| `scheduler/` | agent-B | Go: policy engine, gateway, k8s lifecycle, fission functions |
| `manifests/` | agent-C | Kustomize bases: router (LiteLLM), scheduler, fission, models, rbac, services |
| `fission/` | agent-C | Fission function source/packages (or under `scheduler/pkg/fission`) |

Only edit files under your assigned path. The parent merges branches.

## Conventions

- **Nix / devenv**: `use flake` in `.envrc`; shell provided by `flake.nix` +
  `devenv.nix`. Tools: `generate-manifests`, `import-crds`, `go build/test`, `nix run .#conductor -- server`.
- **Go**: module `github.com/projectinitiative/conductor`. Multi-headed binary
  (`conductor server` | `conductor function <op>`). Keep the policy engine
  **pure & clock-injectable**; test with a fake clock (no cluster).
- **cdk8s**: emit resources via `ApiObject` (raw YAML) so no pre-generated CRD
  stubs are required. `import-crds` is optional.
- **Formatting**: `nixfmt` for `.nix`, `gofmt` for Go, `ruff` for Python.
  The flake `checks.tests` runs `go test ./...`; `checks.formatting` checks nixfmt/gofmt.
- **Unit tests only.** No integration tests. Do not try to touch a real cluster.

## Definition of done per agent

1. Code compiles / generator runs.
2. Unit tests pass (`go test ./...` or `pytest`).
3. `gofmt`/`nixfmt` clean.
4. Commit on your worktree branch with a clear message.
