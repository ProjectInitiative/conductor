#!/usr/bin/env python3
"""Conductor — Argo CD Application manifest generator (cdk8s).

Reads a declarative catalog (apps.yaml) and cluster deployment config
(clusters/*.yaml) and emits Argo CD `Application` CRDs for the LLM
model-serving stack (router, scheduler, fission, models, rbac, services).

Pieces are emitted with `ApiObject` (raw YAML) so the generator runs without
requiring pre-generated cdk8s CRD Python stubs; `import-crds` is only needed if
we later want typed constructors.

The generator is also importable: `build_application_spec(...)` and
`build_sources(...)` are pure helpers the tests exercise without cdk8s.
"""

import copy
import os
import sys

import yaml
from cdk8s import App, Chart, ApiObject, JsonPatch
from constructs import Construct


def load_yaml(path):
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        return yaml.safe_load(f)


GENERATOR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(GENERATOR_DIR)
DEFAULT_REPO_URL = "https://github.com/projectinitiative/conductor.git"
DEFAULT_REVISION = "HEAD"
PROFILES_CM = "conductor-model-profiles"


# ---------------------------------------------------------------------------
# Pure spec builders (unit-testable, no cdk8s)
# ---------------------------------------------------------------------------
def _deep_merge(a, b):
    """Recursively merge b into a (returns a new dict). b wins on scalars."""
    out = copy.deepcopy(a or {})
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def _merge_kustomize(base, extra):
    """Merge kustomize settings, extending images/patches lists."""
    merged = copy.deepcopy(base or {})
    for k, v in (extra or {}).items():
        if k in ("images", "patches") and isinstance(v, list):
            merged.setdefault(k, [])
            merged[k] = merged[k] + copy.deepcopy(v)
        else:
            merged[k] = copy.deepcopy(v)
    return merged


def _image_overrides(deploy):
    """Turn deploy.image {name,newTag} (and optional newName) into kustomize images."""
    img = deploy.get("image")
    if not img:
        return {}
    entry = {"name": img["name"], "newTag": img.get("newTag", img.get("tag", "latest"))}
    if img.get("newName"):
        entry["newName"] = img["newName"]
    return {"images": [entry]}


def _profiles_patch(model_profiles, namespace):
    """Build a kustomize patch that fills the conductor-model-profiles ConfigMap."""
    if not model_profiles:
        return None
    data_lines = []
    for name, profile in model_profiles.items():
        # Each profile is serialized as a YAML doc inside the ConfigMap data.
        data_lines.append(f"  {name}: |")
        body = yaml.safe_dump(profile, default_flow_style=False, sort_keys=True)
        for line in body.rstrip("\n").split("\n"):
            data_lines.append(f"    {line}")
    data_block = "\n".join(data_lines)
    patch_yaml = (
        "apiVersion: v1\n"
        "kind: ConfigMap\n"
        "metadata:\n"
        f"  name: {PROFILES_CM}\n"
        f"  namespace: {namespace}\n"
        "data:\n"
        f"{data_block}\n"
    )
    return {
        "patch": patch_yaml,
        "target": {"kind": "ConfigMap", "name": PROFILES_CM},
    }


def build_sources(app_def, deploy, model_profiles, namespace, defaults):
    """Return the list of Argo CD sources for one application."""
    repo_url = deploy.get("repoURL", app_def.get("repoURL", defaults.get("repoURL", DEFAULT_REPO_URL)))
    revision = deploy.get("targetRevision", app_def.get("targetRevision", defaults.get("targetRevision", DEFAULT_REVISION)))
    sources = []

    if "chart" in app_def:
        # Helm chart source.
        src = {
            "repoURL": app_def.get("chartRepoURL", app_def.get("repoURL", repo_url)),
            "chart": app_def["chart"],
            "targetRevision": app_def.get("targetRevision", revision),
        }
        helm = {}
        if deploy.get("helm_values"):
            helm = {"values": deploy["helm_values"]}
        unless_empty = deploy.get("helm", {}) or app_def.get("helm", {})
        if unless_empty:
            helm = _deep_merge(unless_empty, helm)
        if helm:
            src["helm"] = helm
        sources.append(src)
        return sources

    # git-path source (Kustomize base in this repo).
    path = deploy.get("path", app_def.get("path"))
    if not path:
        return sources
    src = {"repoURL": repo_url, "targetRevision": revision, "path": path}

    kustomize = _merge_kustomize(
        app_def.get("kustomize", {}),
        deploy.get("kustomize", {}),
    )
    kustomize = _merge_kustomize(kustomize, _image_overrides(deploy))

    # For the models app, carry the per-model scheduler profiles.
    if path.startswith("manifests/models"):
        patch = _profiles_patch(model_profiles, namespace)
        if patch:
            kustomize = _merge_kustomize(kustomize, {"patches": [patch]})

    if kustomize:
        src["kustomize"] = kustomize
    if deploy.get("helm_values"):
        src["helm"] = {"values": deploy["helm_values"]}
    sources.append(src)
    return sources


def build_application(app_name, app_def, deploy, model_profiles, cluster_config, defaults):
    """Return a complete Argo CD Application spec dict for (app, cluster)."""
    server_url = cluster_config["server"]
    namespace = deploy.get("namespace", "llm-test")
    project = deploy.get("project", cluster_config.get("project", "conductor"))
    sync_policy_def = app_def.get("syncPolicy", {}) or {}

    sources = build_sources(app_def, deploy, model_profiles, namespace, defaults)

    sync_options = ["CreateNamespace=true", "ServerSideApply=true"]
    additional = list(deploy.get("syncOptions") or []) + list(sync_policy_def.get("syncOptions") or [])
    for opt in additional:
        if opt not in sync_options:
            sync_options.append(opt)

    sync_policy = {"syncOptions": sync_options}
    automated = sync_policy_def.get("automated", {"prune": True, "selfHeal": True})
    if automated:
        sync_policy["automated"] = automated

    return {
        "project": project,
        "destination": {"server": server_url, "namespace": namespace},
        "sources": sources,
        "syncPolicy": sync_policy,
    }


# ---------------------------------------------------------------------------
# cdk8s chart
# ---------------------------------------------------------------------------
class ConductorChart(Chart):
    """One Chart per cluster. Emits Argo CD Application CRDs for that cluster."""

    def __init__(self, scope: Construct, cluster_file: str, catalog: dict,
                 model_profiles: dict, defaults: dict):
        cluster_config = load_yaml(cluster_file)
        if not cluster_config:
            return

        cluster_name = cluster_config["name"]
        argo_namespace = cluster_config.get("argoNamespace", "argocd")

        super().__init__(scope, cluster_name, disable_resource_name_hashes=True)
        print(f"Processing cluster: {cluster_name}", file=sys.stderr)

        for app in cluster_config.get("apps", []):
            app_name = app["name"]
            app_def = catalog.get(app_name)
            if not app_def:
                print(f"  [WARN] '{app_name}' not in catalog. Skipping.", file=sys.stderr)
                continue

            spec = build_application(app_name, app_def, app, model_profiles,
                                     cluster_config, defaults)

            obj = ApiObject(
                self,
                f"{cluster_name}-{app_name}",
                api_version="argoproj.io/v1alpha1",
                kind="Application",
                metadata={
                    "name": app_name,
                    "namespace": argo_namespace,
                    "labels": {
                        "app.kubernetes.io/part-of": "conductor",
                        "conductor.io/cluster": cluster_name,
                    },
                },
            )
            obj.add_json_patch(JsonPatch.add("/spec", spec))


def main():
    catalog_doc = load_yaml(os.path.join(ROOT_DIR, "apps.yaml")) or {}
    catalog = catalog_doc.get("catalog", {})
    defaults = catalog_doc.get("defaults", {})
    model_profiles = catalog_doc.get("modelProfiles", {})

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
        ConductorChart(app, cluster_file, catalog, model_profiles, defaults)

    app.synth()
    print("Manifests synthesized.", file=sys.stderr)


if __name__ == "__main__":
    main()
