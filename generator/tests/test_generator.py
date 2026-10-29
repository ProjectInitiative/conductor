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
    assert "ResourcePool" in kinds
    assert "ManagedWorkload" in kinds
    assert "Deployment" in kinds
    assert "ServiceAccount" in kinds
    assert "Role" in kinds and "RoleBinding" in kinds
    assert "ClusterRole" in kinds and "ClusterRoleBinding" in kinds
    assert "PodDisruptionBudget" in kinds

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
    model_download_rule = next(rule for rule in rules if rule["resources"] == ["modeldownloads"])
    assert model_download_rule["verbs"] == ["get", "list", "watch"]
    assert any("modeldownloads/status" in rule["resources"] and "patch" in rule["verbs"] for rule in rules)
    assert any(rule["resources"] == ["modeldownloads/finalizers"] and rule["verbs"] == ["update"] for rule in rules)
    assert any(rule["resources"] == ["jobs"] and "create" in rule["verbs"] and "delete" in rule["verbs"] for rule in rules)
    assert any(rule["resources"] == ["persistentvolumeclaims"] and "create" in rule["verbs"] and "delete" in rule["verbs"] for rule in rules)
    node_role = by_kind(objs, "ClusterRole", "conductor-node-reader")
    assert node_role["rules"] == [{"apiGroups": [""], "resources": ["nodes"], "verbs": ["get", "list", "watch"]}]
    downloader_role = by_kind(objs, "ClusterRole", "conductor-downloader-reader")
    assert downloader_role["rules"] == [{"apiGroups": ["conductor.projectinitiative.io"], "resources": ["downloaders"], "verbs": ["get", "list", "watch"]}]


def test_policy_crs_are_homelab_owned(tmp_path):
    """ResourcePool/ManagedWorkload policy lives in homelab's
    conductor-workloads Application, not in this baseline. Duplicated
    ownership made Argo report SharedResourceWarning for the same objects
    with diverging specs (Observe/100 baseline vs Manage/0 homelab)."""
    objs = run_generator(tmp_path)
    assert by_kind(objs, "ResourcePool", "dgx-spark") is None
    assert by_kind(objs, "ManagedWorkload", "tensorfold-v14") is None


def test_crds_have_status_subresources(tmp_path):
    objs = run_generator(tmp_path)
    crds = [obj for obj in objs if obj["kind"] == "CustomResourceDefinition"]
    assert {crd["metadata"]["name"] for crd in crds} == {
        "resourcepools.conductor.projectinitiative.io",
        "managedworkloads.conductor.projectinitiative.io",
        "workloadoverrides.conductor.projectinitiative.io",
        "modeldownloads.conductor.projectinitiative.io",
        "downloaders.conductor.projectinitiative.io",
        "packagepreparations.conductor.projectinitiative.io",
        "preparerclasses.conductor.projectinitiative.io",
    }
    for crd in crds:
        assert crd["spec"]["versions"][0]["subresources"] == {"status": {}}

    model_download = by_kind(objs, "CustomResourceDefinition", "modeldownloads.conductor.projectinitiative.io")
    model_version = model_download["spec"]["versions"][0]
    assert model_download["spec"]["scope"] == "Namespaced"
    model_spec = model_version["schema"]["openAPIV3Schema"]["properties"]["spec"]["properties"]
    assert model_spec["operation"]["enum"] == ["Ensure", "Delete"]
    assert model_spec["rateLimit"]["properties"]["bytesPerSecond"]["minimum"] == 0
    assert model_spec["workers"]["properties"]["fileWorkers"]["minimum"] == 1
    assert model_version["schema"]["openAPIV3Schema"]["type"] == "object"

    downloader = by_kind(objs, "CustomResourceDefinition", "downloaders.conductor.projectinitiative.io")
    downloader_version = downloader["spec"]["versions"][0]
    assert downloader["spec"]["scope"] == "Cluster"
    image_pull_policy = downloader_version["schema"]["openAPIV3Schema"]["properties"]["spec"]["properties"]["imagePullPolicy"]
    assert image_pull_policy["enum"] == ["IfNotPresent", "Always", "Never"]
    assert image_pull_policy["default"] == "IfNotPresent"
