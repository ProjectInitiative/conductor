"""Tests for the Conductor cdk8s generator (self-contained deployable)."""

import io
import os

import yaml

import generator.main as gen


def yaml_load_all(text):
    return list(yaml.safe_load_all(io.StringIO(text)))


def run_generator(tmp_path):
    outdir = str(tmp_path / "out")
    os.environ["MANIFEST_OUTPUT_DIR"] = outdir
    gen.main()
    objs = []
    for name in os.listdir(outdir):
        if not name.endswith(".yaml"):
            continue
        with open(os.path.join(outdir, name)) as f:
            for doc in yaml_load_all(f.read()):
                if doc and doc.get("kind"):
                    objs.append(doc)
    return objs


def by_kind(objs, kind, name=None):
    for o in objs:
        if o["kind"] == kind and (name is None or o["metadata"]["name"] == name):
            return o
    return None


def test_renders_full_deployable_stack(tmp_path):
    objs = run_generator(tmp_path)
    kinds = {o["kind"] for o in objs}
    # Kubernetes primitives
    assert "Namespace" in kinds
    assert "Deployment" in kinds
    assert "Service" in kinds
    assert "ConfigMap" in kinds
    assert "ServiceAccount" in kinds
    assert "Role" in kinds and "RoleBinding" in kinds
    assert "PersistentVolumeClaim" in kinds
    # Fission CRs
    assert "Environment" in kinds
    assert "Function" in kinds
    assert "HTTPTrigger" in kinds
    assert "KubernetesWatchTrigger" in kinds


def test_conductor_scheduler_deployment(tmp_path):
    objs = run_generator(tmp_path)
    dep = by_kind(objs, "Deployment", "conductor")
    assert dep is not None
    c = dep["spec"]["template"]["spec"]["containers"][0]
    assert c["image"] == "ghcr.io/projectinitiative/conductor:0.1.0"
    assert c["command"] == ["/bin/conductor", "server"]
    assert dep["spec"]["template"]["spec"]["serviceAccountName"] == "conductor"


def test_router_and_tailscale(tmp_path):
    objs = run_generator(tmp_path)
    assert by_kind(objs, "Deployment", "litellm") is not None
    ai = by_kind(objs, "Service", "ai")
    assert ai is not None
    assert ai["spec"]["loadBalancerClass"] == "tailscale"
    assert ai["spec"]["selector"] == {"app": "litellm"}


def test_rbac_allows_scale(tmp_path):
    objs = run_generator(tmp_path)
    role = by_kind(objs, "Role", "conductor")
    rules = role["rules"]
    assert any("deployments/scale" in r["resources"] for r in rules)


def test_model_profiles_configmap_has_three_models(tmp_path):
    objs = run_generator(tmp_path)
    cm = by_kind(objs, "ConfigMap", "conductor-model-profiles")
    assert cm is not None
    txt = cm["data"]["models.yaml"]
    for name in ["deepseek-v4-flash-vision-exp", "GLM-5.3-Flash-EXL3", "qwen3.8-flash-next"]:
        assert name in txt
    assert "deepseek-parity-head" in txt
    assert "glm-parity-head" in txt
    assert "qwen-parity-head" in txt


def test_model_templates_are_scaled_to_zero(tmp_path):
    objs = run_generator(tmp_path)
    deps = [o for o in objs if o["kind"] == "Deployment"
            and o["metadata"]["name"] in ("deepseek-parity-head", "deepseek-parity-worker",
                                          "glm-parity-head", "glm-parity-worker",
                                          "qwen-parity-head", "qwen-parity-worker")]
    assert len(deps) == 6
    for d in deps:
        assert d["spec"]["replicas"] == 0
        assert d["spec"]["template"]["spec"]["hostNetwork"] is True


def test_fission_functions_present(tmp_path):
    objs = run_generator(tmp_path)
    assert by_kind(objs, "Function", "conductor-status") is not None
    assert by_kind(objs, "Function", "conductor-model-ready") is not None
    env = by_kind(objs, "Environment", "conductor")
    assert env is not None
    assert env["spec"]["runtime"]["container"]["command"] == ["/bin/conductor", "function"]
