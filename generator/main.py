#!/usr/bin/env python3
"""Conductor — Argo CD Application manifest generator (cdk8s).

Reads a declarative catalog (apps.yaml) and cluster deployment config
(clusters/*.yaml) and emits Argo CD `Application` CRDs for the LLM
model-serving stack.

Pieces are emitted with `ApiObject` (raw YAML) so the generator runs without
requiring pre-generated cdk8s CRD Python stubs; `import-crds` is only needed if
we later want typed constructors.
"""

import os
import sys

import yaml
from cdk8s import App, Chart, ApiObject
from constructs import Construct


def load_yaml(path):
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        return yaml.safe_load(f)


GENERATOR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(GENERATOR_DIR)


class ConductorChart(Chart):
    """One Chart per cluster. Emits Argo CD Application CRDs for that cluster."""

    def __init__(self, scope: Construct, cluster_file: str, catalog: dict):
        cluster_config = load_yaml(cluster_file)
        if not cluster_config:
            return

        cluster_name = cluster_config["name"]
        server_url = cluster_config["server"]
        argo_namespace = cluster_config.get("argoNamespace", "argocd")
        project = cluster_config.get("project", "conductor")

        super().__init__(scope, cluster_name, disable_resource_name_hashes=True)
        print(f"Processing cluster: {cluster_name} ({server_url})", file=sys.stderr)

        for app in cluster_config.get("apps", []):
            app_name = app["name"]
            app_def = catalog.get(app_name)
            if not app_def:
                print(f"  [WARN] '{app_name}' not in catalog. Skipping.", file=sys.stderr)
                continue

            self._emit_application(
                cluster_name=cluster_name,
                server_url=server_url,
                argo_namespace=argo_namespace,
                project=project,
                app_name=app_name,
                app_def=app_def,
                deploy=app,
            )

    def _emit_application(self, cluster_name, server_url, argo_namespace, project,
                          app_name, app_def, deploy):
        # Source: a git path in this repo, or a Helm chart, or an image override.
        sources = self._build_sources(app_name, app_def, deploy)

        ApiObject(
            self,
            f"{cluster_name}-{app_name}",
            api_version="argoproj.io/v1alpha1",
            kind="Application",
            metadata={
                "name": f"{app_name}",
                "namespace": argo_namespace,
                "labels": {"app.kubernetes.io/part-of": "conductor",
                           "conductor.io/cluster": cluster_name},
            },
            spec={
                "project": project,
                "destination": {"server": server_url,
                                 "namespace": deploy.get("namespace", "llm-test")},
                "sources": sources,
                "syncPolicy": {
                    "syncOptions": ["CreateNamespace=true", "ServerSideApply=true"],
                    "automated": {"prune": True, "selfHeal": True},
                },
            },
        )

    def _build_sources(self, app_name, app_def, deploy):
        sources = []
        repo_url = deploy.get("repoURL", app_def.get("repoURL", "https://github.com/projectinitiative/conductor.git"))
        revision = deploy.get("targetRevision", app_def.get("targetRevision", "HEAD"))

        # A git-path source (Kustomize base in this repo) or a Helm source.
        if "path" in app_def or "path" in deploy:
            path = deploy.get("path", app_def.get("path"))
            src = {
                "repoURL": repo_url,
                "targetRevision": revision,
                "path": path,
            }
            if app_def.get("kustomize") or deploy.get("kustomize"):
                src["kustomize"] = deploy.get("kustomize", app_def.get("kustomize", {}))
            if deploy.get("helm_values"):
                src["helm"] = {"values": deploy["helm_values"]}
            sources.append(src)
        elif "chart" in app_def:
            src = {
                "repoURL": app_def.get("repoURL", repo_url),
                "chart": app_def["chart"],
                "targetRevision": app_def.get("targetRevision", revision),
            }
            if app_def.get("helm") or deploy.get("helm_values"):
                src["helm"] = deploy.get("helm", app_def.get("helm", {}))
            sources.append(src)

        # Optional image override (scheduler / fission built image).
        if deploy.get("image"):
            sources.append({
                "repoURL": repo_url,
                "targetRevision": revision,
                "path": "manifests/common",
                "kustomize": {
                    "images": [{"name": deploy["image"]["name"],
                                "newTag": deploy["image"]["newTag"]}],
                },
            })
        return sources


def main():
    apps_catalog = load_yaml(os.path.join(ROOT_DIR, "apps.yaml")) or {}
    catalog = apps_catalog.get("catalog", {})

    app = App(outdir=os.environ.get("MANIFEST_OUTPUT_DIR", ".direnv/manifests"))
    cluster_dir = os.path.join(ROOT_DIR, "clusters")
    cluster_files = sorted(
        os.path.join(cluster_dir, f)
        for f in os.listdir(cluster_dir)
        if f.endswith(".yaml")
    )
    if not cluster_files:
        print("Error: no clusters/*.yaml found", file=sys.stderr)
        sys.exit(1)

    for cluster_file in cluster_files:
        ConductorChart(app, cluster_file, catalog)

    app.synth()
    print("Manifests synthesized.", file=sys.stderr)


if __name__ == "__main__":
    main()
