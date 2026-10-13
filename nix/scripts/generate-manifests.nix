{ pkgs, pythonEnv }:
pkgs.writeShellScriptBin "generate-manifests" ''
  set -euo pipefail
  export PATH="${pythonEnv}/bin:${pkgs.nodejs_22}/bin:$PATH"

  if [ -z "''${MANIFEST_OUTPUT_DIR:-}" ]; then
    export MANIFEST_OUTPUT_DIR=$(pwd)/.direnv/manifests
  fi
  mkdir -p "$MANIFEST_OUTPUT_DIR"

  echo "Generating manifests to $MANIFEST_OUTPUT_DIR..."
  cd generator
  ${pythonEnv}/bin/python main.py
  echo "Manifests generated in $MANIFEST_OUTPUT_DIR"
''
