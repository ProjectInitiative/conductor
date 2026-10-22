#!/usr/bin/env python3
"""Conductor — cdk8s generator.

Renders the deployable Conductor control plane (CRDs, RBAC, HA controller
Deployment + PDB, and baseline ResourcePool/ManagedWorkload custom resources)
from a single declarative source of truth. This is what Argo CD runs via the
`cdk8s-v1.0` CMP plugin: the generator here decides how everything is
assembled, and Argo CD just applies the result.

Existing model Deployments stay homelab-owned; the operator references them
explicitly. No Helm chart, no hand-written Kustomize for the deployables.
"""

import os
import sys

import yaml
from cdk8s import ApiObject, App, Chart, JsonPatch
from constructs import Construct


def load_yaml(path):
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        return yaml.safe_load(f)


GENERATOR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(GENERATOR_DIR)


class ConductorStack(Chart):
    """Renders the whole self-contained Conductor app in one namespace."""

    def __init__(self, scope: Construct, cfg: dict, deploy: dict):
        namespace = deploy.get("namespace", "llm-test")
        image = deploy.get("image", "ghcr.io/projectinitiative/conductor:0.1.0")
        super().__init__(scope, namespace, disable_resource_name_hashes=True)
        print(f"Rendering Conductor stack in namespace {namespace}", file=sys.stderr)

        self._namespace(namespace)
        self._crds()
        self._rbac(namespace)
        self._controller(namespace, image)

        # The Kubernetes-native operator starts with reference-only ownership
        # of explicitly declared workloads. It never renders model Deployments,
        # routers, or watch triggers: those are homelab-owned resources.
        self._baseline(namespace, cfg)

    # ---- emit helper ----------------------------------------------------
    def _emit(self, name, api_version, kind, manifest):
        meta = manifest.get("metadata", {"name": name})
        obj = ApiObject(self, name, api_version=api_version, kind=kind, metadata=meta)
        for key, val in manifest.items():
            if key in ("apiVersion", "kind", "metadata"):
                continue
            obj.add_json_patch(JsonPatch.add(f"/{key}", val))
        return obj

    def _namespace(self, ns):
        self._emit(ns, "v1", "Namespace", {
            "metadata": {"name": ns, "labels": {"purpose": "conductor", "vault-auth": "enabled"}},
        })

    # ---- CRDs -----------------------------------------------------------
    def _crds(self):
        crd_dir = os.path.join(GENERATOR_DIR, "crds")
        for filename in sorted(os.listdir(crd_dir)):
            if not filename.endswith(".yaml"):
                continue
            manifest = load_yaml(os.path.join(crd_dir, filename))
            self._emit("crd-" + manifest["metadata"]["name"].replace(".", "-"),
                       manifest["apiVersion"], manifest["kind"], manifest)

    # ---- RBAC -----------------------------------------------------------
    def _rbac(self, ns):
        self._emit("conductor-sa", "v1", "ServiceAccount",
                   {"metadata": {"name": "conductor", "namespace": ns}})
        self._emit("conductor-role", "rbac.authorization.k8s.io/v1", "Role", {
            "metadata": {"name": "conductor", "namespace": ns},
            "rules": [
                {"apiGroups": ["conductor.projectinitiative.io"],
                 "resources": ["resourcepools", "managedworkloads", "workloadoverrides"],
                 "verbs": ["get", "list", "watch"]},
                {"apiGroups": ["conductor.projectinitiative.io"],
                 "resources": ["modeldownloads"],
                 "verbs": ["get", "list", "watch"]},
                {"apiGroups": ["conductor.projectinitiative.io"],
                 "resources": ["packagepreparations"],
                 "verbs": ["get", "list", "watch"]},
                {"apiGroups": ["conductor.projectinitiative.io"],
                 "resources": ["resourcepools/status", "managedworkloads/status", "workloadoverrides/status", "modeldownloads/status", "packagepreparations/status"],
                 "verbs": ["get", "update", "patch"]},
                {"apiGroups": ["conductor.projectinitiative.io"],
                 "resources": ["modeldownloads/finalizers"],
                 "verbs": ["update"]},
                {"apiGroups": ["apps"], "resources": ["deployments", "replicasets"],
                 "verbs": ["get", "list", "watch"]},
                {"apiGroups": ["apps"], "resources": ["deployments/scale"],
                 "verbs": ["get", "patch", "update"]},
                {"apiGroups": [""], "resources": ["pods", "services"],
                 "verbs": ["get", "list", "watch"]},
                {"apiGroups": ["batch"], "resources": ["jobs"],
                 "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"]},
                {"apiGroups": [""], "resources": ["persistentvolumeclaims"],
                 "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"]},
                {"apiGroups": ["discovery.k8s.io"], "resources": ["endpointslices"],
                 "verbs": ["get", "list", "watch"]},
                {"apiGroups": [""], "resources": ["events"],
                 "verbs": ["create", "patch", "update"]},
                {"apiGroups": ["coordination.k8s.io"], "resources": ["leases"],
                 "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"]},
            ],
        })
        self._emit("conductor-rolebinding", "rbac.authorization.k8s.io/v1", "RoleBinding", {
            "metadata": {"name": "conductor", "namespace": ns},
            "subjects": [{"kind": "ServiceAccount", "name": "conductor", "namespace": ns}],
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "conductor"},
        })
        self._emit("conductor-node-reader", "rbac.authorization.k8s.io/v1", "ClusterRole", {
            "metadata": {"name": "conductor-node-reader"},
            "rules": [{"apiGroups": [""], "resources": ["nodes"], "verbs": ["get", "list", "watch"]}],
        })
        self._emit("conductor-node-reader-binding", "rbac.authorization.k8s.io/v1", "ClusterRoleBinding", {
            "metadata": {"name": "conductor-node-reader"},
            "subjects": [{"kind": "ServiceAccount", "name": "conductor", "namespace": ns}],
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "conductor-node-reader"},
        })
        self._emit("conductor-downloader-reader", "rbac.authorization.k8s.io/v1", "ClusterRole", {
            "metadata": {"name": "conductor-downloader-reader"},
            "rules": [{"apiGroups": ["conductor.projectinitiative.io"],
                       "resources": ["downloaders"], "verbs": ["get", "list", "watch"]}],
        })
        self._emit("conductor-downloader-reader-binding", "rbac.authorization.k8s.io/v1", "ClusterRoleBinding", {
            "metadata": {"name": "conductor-downloader-reader"},
            "subjects": [{"kind": "ServiceAccount", "name": "conductor", "namespace": ns}],
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "conductor-downloader-reader"},
        })

    # ---- Kubernetes-native controller ----------------------------------
    def _controller(self, ns, image):
        labels = {"app.kubernetes.io/name": "conductor", "app.kubernetes.io/component": "controller"}
        self._emit("conductor-controller", "apps/v1", "Deployment", {
            "metadata": {"name": "conductor-controller", "namespace": ns},
            "spec": {
                "replicas": 2,
                "selector": {"matchLabels": labels},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "serviceAccountName": "conductor",
                        "terminationGracePeriodSeconds": 30,
                        "securityContext": {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}},
                        "topologySpreadConstraints": [{
                            "maxSkew": 1,
                            "topologyKey": "kubernetes.io/hostname",
                            "whenUnsatisfiable": "ScheduleAnyway",
                            "labelSelector": {"matchLabels": labels},
                        }],
                        "containers": [{
                            "name": "controller",
                            "image": image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["/bin/conductor", "controller"],
                            "ports": [
                                {"name": "metrics", "containerPort": 8080},
                                {"name": "health", "containerPort": 8081},
                            ],
                            "readinessProbe": {"httpGet": {"path": "/readyz", "port": "health"}, "periodSeconds": 10},
                            "livenessProbe": {"httpGet": {"path": "/healthz", "port": "health"}, "periodSeconds": 20},
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "resources": {
                                "requests": {"cpu": "50m", "memory": "96Mi"},
                                "limits": {"cpu": "500m", "memory": "256Mi"},
                            },
                        }],
                    },
                },
            },
        })
        self._emit("conductor-controller-pdb", "policy/v1", "PodDisruptionBudget", {
            "metadata": {"name": "conductor-controller", "namespace": ns},
            "spec": {"maxUnavailable": 1, "selector": {"matchLabels": labels}},
        })

    def _baseline(self, ns, cfg):
        for name, spec in cfg.get("resourcePools", {}).items():
            self._emit("pool-" + name, "conductor.projectinitiative.io/v1alpha1", "ResourcePool", {
                "metadata": {"name": name, "namespace": ns}, "spec": spec,
            })
        for name, spec in cfg.get("managedWorkloads", {}).items():
            self._emit("workload-" + name, "conductor.projectinitiative.io/v1alpha1", "ManagedWorkload", {
                "metadata": {"name": name, "namespace": ns}, "spec": spec,
            })
        for name, spec in cfg.get("downloaders", {}).items():
            self._emit("downloader-" + name, "conductor.projectinitiative.io/v1alpha1", "DownloaderClass", {
                "metadata": {"name": name}, "spec": spec,
            })
        for name, spec in cfg.get("modelDownloads", {}).items():
            self._emit("model-download-" + name, "conductor.projectinitiative.io/v1alpha1", "ModelDownload", {
                "metadata": {"name": name, "namespace": ns}, "spec": spec,
            })
        for name, spec in cfg.get("preparerClasses", {}).items():
            self._emit("preparer-" + name, "conductor.projectinitiative.io/v1alpha1", "PreparerClass", {
                "metadata": {"name": name}, "spec": spec,
            })
        for name, spec in cfg.get("packagePreparations", {}).items():
            self._emit("package-preparation-" + name, "conductor.projectinitiative.io/v1alpha1", "PackagePreparation", {
                "metadata": {"name": name, "namespace": ns}, "spec": spec,
            })


def main():
    cfg = load_yaml(os.path.join(ROOT_DIR, "apps.yaml")) or {}
    cluster_dir = os.path.join(ROOT_DIR, "clusters")
    cluster_files = sorted(
        os.path.join(cluster_dir, f)
        for f in os.listdir(cluster_dir)
        if f.endswith(".yaml")
    )
    if not cluster_files:
        print("Error: no clusters/*.yaml found", file=sys.stderr)
        sys.exit(1)

    outdir = os.environ.get("MANIFEST_OUTPUT_DIR") or os.environ.get("CDK8S_OUTDIR") or "dist"
    app = App(outdir=outdir)
    for cluster_file in cluster_files:
        cluster = load_yaml(cluster_file) or {}
        for deploy in cluster.get("apps", []):
            ConductorStack(app, cfg, deploy)
    app.synth()
    print("Conductor deployable rendered.", file=sys.stderr)


if __name__ == "__main__":
    main()
