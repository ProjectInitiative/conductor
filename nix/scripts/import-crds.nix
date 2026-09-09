{ pkgs }:
pkgs.writeShellScriptBin "import-crds" ''
  set -euo pipefail
  cd generator
  ${pkgs.cdk8s-cli}/bin/cdk8s import
  echo "CRDs imported to generator/imports/"
''
