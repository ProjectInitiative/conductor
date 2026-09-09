{ pkgs, config, lib, ... }:

let
  generatorEnv = config.languages.python.import ./generator { };
in
{
  languages.python = {
    enable = true;
    uv.enable = true;
  };

  languages.go = {
    enable = true;
  };

  packages = [
    pkgs.nodejs_22
    pkgs.cdk8s-cli
    pkgs.go
    pkgs.gopls
    pkgs.gotools
    pkgs.delta
  ];

  scripts.generate-manifests.exec = ''
    export MANIFEST_OUTPUT_DIR="''${MANIFEST_OUTPUT_DIR:-$(pwd)/.direnv/manifests}"
    mkdir -p "$MANIFEST_OUTPUT_DIR"
    ( cd generator && ${generatorEnv}/bin/python main.py )
  '';

  scripts.import-crds.exec = ''
    cd generator
    ${pkgs.cdk8s-cli}/bin/cdk8s import
  '';

  enterShell = ''
    echo "╔═══════════════════════════════════════════════╗"
    echo "║        Conductor Development Shell             ║"
    echo "╚═══════════════════════════════════════════════╝"
    echo ""
    echo "  cdk8s:   generate-manifests | import-crds"
    echo "  Go:      go build ./... | go test ./... (in scheduler/)"
    echo "  Run:     nix run .#conductor -- server"
    echo ""
  '';

  enterTest = ''
    ${generatorEnv}/bin/python -c "from cdk8s import App; print('cdk8s OK')"
    ( cd scheduler && go test ./... )
  '';
}
