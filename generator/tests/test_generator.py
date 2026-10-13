"""Tests for the Kubernetes-native Conductor control plane."""

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
            objs.extend(doc for doc in yaml.safe_load_all(f) if doc)
    return objs


def by_kind(objs, kind, name=None):
    for obj in objs:
        if obj["kind"] == kind and (name is None or obj["metadata"]["name"] == name):
            return obj
    return None


def test_renders_operator_control_plane_only(tmp_path):
    objs = run_generator(tmp_path)
    kinds = {obj["kind"] for obj in objs}
    assert "CustomResourceDefinition" in kinds
    assert "ResourcePool" not in kinds
    assert "ManagedWorkload" not in kinds
    assert "Deployment" in kinds
    assert "ServiceAccount" in kinds
    assert "Role" in kinds and "RoleBinding" in kinds
    assert "ClusterRole" in kinds and "ClusterRoleBinding" in kinds
    assert "PodDisruptionBudget" in kinds

    # llm-test already exists and contains manually managed experiment lanes.
    # The operator Application must never own or prune that shared Namespace.
    assert by_kind(objs, "Namespace", "llm-test") is None

    # Legacy scheduler/router/Fission/model resources are deliberately absent.
    assert "Environment" not in kinds
    assert "Function" not in kinds
    assert "HTTPTrigger" not in kinds
    assert by_kind(objs, "Deployment", "litellm") is None
    assert by_kind(objs, "PersistentVolumeClaim", "model-cache") is None


def test_controller_deployment_is_ha_and_architecture_neutral(tmp_path):
    objs = run_generator(tmp_path)
    dep = by_kind(objs, "Deployment", "conductor-controller")
    assert dep is not None
    assert dep["spec"]["replicas"] == 2
    pod = dep["spec"]["template"]["spec"]
    assert "nodeSelector" not in pod
    assert pod["serviceAccountName"] == "conductor"
    container = pod["containers"][0]
    assert container["command"] == ["/bin/conductor", "controller"]
    assert container["image"] == (
        "ghcr.io/projectinitiative/conductor@"
        "sha256:1f2771564902c03b90a77aa4efb13f8cf45b7919d4cd7d7ff4a167dfd0737e04"
    )
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert container["securityContext"]["capabilities"]["drop"] == ["ALL"]


def test_rbac_is_scoped_for_native_reconciliation(tmp_path):
    objs = run_generator(tmp_path)
    role = by_kind(objs, "Role", "conductor")
    rules = role["rules"]
    assert any("deployments/scale" in rule["resources"] for rule in rules)
    assert any("replicasets" in rule["resources"] for rule in rules)
    assert any("pods" in rule["resources"] and rule["verbs"] == ["get", "list", "watch"] for rule in rules)
    assert not any("delete" in rule["verbs"] for rule in rules if "pods" in rule["resources"])
    assert any("endpointslices" in rule["resources"] for rule in rules)
    node_role = by_kind(objs, "ClusterRole", "conductor-node-reader")
    assert node_role["rules"] == [{"apiGroups": [""], "resources": ["nodes"], "verbs": ["get", "list", "watch"]}]



def test_crds_have_status_subresources(tmp_path):
    objs = run_generator(tmp_path)
    crds = [obj for obj in objs if obj["kind"] == "CustomResourceDefinition"]
    assert {crd["metadata"]["name"] for crd in crds} == {
        "resourcepools.conductor.projectinitiative.io",
        "managedworkloads.conductor.projectinitiative.io",
        "workloadoverrides.conductor.projectinitiative.io",
    }
    for crd in crds:
        assert crd["spec"]["versions"][0]["subresources"] == {"status": {}}
