{
  description = "Conductor — LLM model-serving router & swap scheduler for 2x DGX Spark";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    flake-parts.inputs.nixpkgs-lib.follows = "nixpkgs";
    devenv.url = "github:cachix/devenv";
    devenv.inputs.nixpkgs.follows = "nixpkgs";
    nix2container.url = "github:nlewo/nix2container";
    nix2container.inputs.nixpkgs.follows = "nixpkgs";
    mk-shell-bin.url = "github:rrbutani/nix-mk-shell-bin";
    ops-utils.url = "github:projectinitiative/ops-utils";
    uv2nix.url = "github:pyproject-nix/uv2nix";
    uv2nix.inputs.nixpkgs.follows = "nixpkgs";
    pyproject-nix.url = "github:pyproject-nix/pyproject.nix";
    pyproject-nix.inputs.nixpkgs.follows = "nixpkgs";
    pyproject-build-systems.url = "github:pyproject-nix/build-system-pkgs";
    pyproject-build-systems.inputs.nixpkgs.follows = "nixpkgs";
    pyproject-build-systems.inputs.uv2nix.follows = "uv2nix";
    pyproject-build-systems.inputs.pyproject-nix.follows = "pyproject-nix";
  };

  nixConfig = {
    extra-trusted-public-keys = "devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw=";
    extra-substituters = "https://devenv.cachix.org";
  };

  outputs =
    inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        inputs.devenv.flakeModule
      ];

      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];

      perSystem =
        { config, pkgs, lib, system, ... }:
        let
          # ------------------------------------------------------------------
          # Python (cdk8s generator) via uv2nix — mirrors homelab/generator
          # ------------------------------------------------------------------
          workspace = inputs.uv2nix.lib.workspace.loadWorkspace {
            workspaceRoot = ./generator;
          };
          overlay = workspace.mkPyprojectOverlay {
            sourcePreference = "wheel";
          };
          pythonBase = pkgs.callPackage inputs.pyproject-nix.build.packages {
            python = pkgs.python311;
          };
          pythonSet = pythonBase.overrideScope (
            lib.composeManyExtensions [
              inputs.pyproject-build-systems.overlays.default
              overlay
            ]
          );
          generatorEnv = pythonSet.mkVirtualEnv "conductor-generator-env" workspace.deps.default;

          # ------------------------------------------------------------------
          # Go (scheduler / fission) via buildGoModule
          # ------------------------------------------------------------------
          version = "0.1.0";
          rev = "dirty";
          mkGoModule =
            { pname, subPackages ? [ "." ] }:
            pkgs.buildGoModule {
              inherit pname subPackages;
              inherit version;
              src = ./scheduler;
              vendorHash = null;
              ldflags = [
                "-s"
                "-w"
                "-X main.Version=${version}+${rev}"
              ];
            };
          scheduler = mkGoModule { pname = "conductor"; };

          generate-manifests = import ./nix/scripts/generate-manifests.nix {
            inherit pkgs;
            pythonEnv = generatorEnv;
          };
          import-crds = import ./nix/scripts/import-crds.nix { inherit pkgs; };
        in
        {
          packages = {
            default = scheduler;
            conductor = scheduler;
            conductor-function = mkGoModule {
              pname = "conductor-function";
              subPackages = [ "./cmd/conductor" ];
            };
            generate-manifests = generate-manifests;
            import-crds = import-crds;
          };

          apps = {
            generate-manifests = {
              type = "app";
              program = "${generate-manifests}/bin/generate-manifests";
            };
            import-crds = {
              type = "app";
              program = "${import-crds}/bin/import-crds";
            };
          };

          devShells.default = config.devenv.shells.default;

          checks = {
            formatting =
              pkgs.runCommand "check-formatting"
                {
                  nativeBuildInputs = with pkgs; [
                    nixfmt
                    go
                    ruff
                  ];
                  src = ./.;
                }
                ''
                  cd $src
                  nixfmt --check *.nix
                  [ -z "$(gofmt -l scheduler/)" ] || { echo "gofmt diff in scheduler/"; exit 1; }
                  RUFF_CACHE_DIR="$TMPDIR" ruff check generator/ 2>/dev/null || true
                  touch $out
                '';

            tests =
              pkgs.runCommand "run-tests"
                {
                  nativeBuildInputs = with pkgs; [ go ];
                  src = ./.;
                }
                ''
                  export HOME="$TMPDIR"
                  cd $src/scheduler
                  go test ./...
                  touch $out
                '';
          };

          formatter = pkgs.nixfmt;
        };
    };
}
