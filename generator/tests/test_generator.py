"""Tests for the Conductor cdk8s generator."""

import os

import yaml

import generator.main as gen


CLUSTER = {
    "name": "mc",
    "server": "https://172.16.1.50:6443",
    "project": "conductor",
    "argoNamespace": "argocd-mc",
}
DEFAULTS = {"repoURL": "https://github.com/projectinitiative/conductor.git",
            "targetRevision": "HEAD"}
PROFILES = {
    "deepseek-v4-flash-vision-exp": {
        "priority": 1,
        "servedModelName": "deepseek-v4-flash-vision-exp",
        "head": {"deployment": "deepseek-parity-head", "hostIP": "172.16.4.55", "port": 8000},
    },
}


def test_build_sources_git_path():
    app_def = {"path": "manifests/scheduler"}
    deploy = {"name": "scheduler", "namespace": "llm-test",
              "image": {"name": "ghcr.io/projectinitiative/conductor", "newTag": "0.1.0"}}
    sources = gen.build_sources(app_def, deploy, PROFILES, "llm-test", DEFAULTS)
    assert len(sources) == 1
    assert sources[0]["path"] == "manifests/scheduler"
    assert sources[0]["repoURL"] == DEFAULTS["repoURL"]
    assert sources[0]["targetRevision"] == "HEAD"
    # Image override is injected into the source's kustomize images.
    assert sources[0]["kustomize"]["images"] == [
        {"name": "ghcr.io/projectinitiative/conductor", "newTag": "0.1.0"}
    ]


def test_build_sources_helm():
    app_def = {
        "chart": "litellm",
        "repoURL": "https://charts.example.com",
        "helm": {"values": "replicas: 1"},
    }
    deploy = {"name": "router", "namespace": "llm-test"}
    sources = gen.build_sources(app_def, deploy, PROFILES, "llm-test", DEFAULTS)
    assert len(sources) == 1
    assert sources[0]["chart"] == "litellm"
    assert sources[0]["repoURL"] == "https://charts.example.com"
    assert sources[0]["helm"]["values"] == "replicas: 1"


def test_application_spec_shapes():
    app_def = {"path": "manifests/rbac", "kustomize": {}}
    deploy = {"name": "rbac", "namespace": "llm-test"}
    spec = gen.build_application("rbac", app_def, deploy, PROFILES, CLUSTER, DEFAULTS)
    assert spec["project"] == "conductor"
    assert spec["destination"] == {"server": CLUSTER["server"], "namespace": "llm-test"}
    assert spec["sources"][0]["path"] == "manifests/rbac"
    assert spec["syncPolicy"]["syncOptions"] == ["CreateNamespace=true", "ServerSideApply=true"]
    assert spec["syncPolicy"]["automated"] == {"prune": True, "selfHeal": True}


def test_models_app_carries_profiles_patch():
    app_def = {"path": "manifests/models", "kustomize": {}}
    deploy = {"name": "models", "namespace": "llm-test"}
    spec = gen.build_application("models", app_def, deploy, PROFILES, CLUSTER, DEFAULTS)
    patches = spec["sources"][0]["kustomize"]["patches"]
    assert len(patches) == 1
    patch = patches[0]
    assert patch["target"] == {"kind": "ConfigMap", "name": "conductor-model-profiles"}
    assert "kind: ConfigMap" in patch["patch"] or "apiVersion: v1" in patch["patch"]


def test_main_generates_applications(tmp_path, monkeypatch):
    outdir = str(tmp_path / "out")
    monkeypatch.setenv("MANIFEST_OUTPUT_DIR", outdir)
    gen.main()

    # Collect every Application resource emitted.
    apps = []
    for name in os.listdir(outdir):
        if not name.endswith(".yaml"):
            continue
        with open(os.path.join(outdir, name)) as f:
            for doc in yaml_load_all(f.read()):
                if doc and doc.get("kind") == "Application":
                    apps.append(doc)

    names = sorted(a["metadata"]["name"] for a in apps)
    assert names == ["fission", "models", "rbac", "router", "scheduler", "services"]
    for a in apps:
        assert a["spec"]["destination"]["server"] == CLUSTER["server"]
        # Every piece resolves to a source path or helm chart leading back into
        # this repo's manifests/ (self-contained deployable pieces).
        srcs = a["spec"]["sources"]
        assert srcs
        assert any(
            s.get("path", "").startswith("manifests/") or "chart" in s
            for s in srcs
        )


def yaml_load_all(text):
    import io
    return list(yaml.safe_load_all(io.StringIO(text)))
