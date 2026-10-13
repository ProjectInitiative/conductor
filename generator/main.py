#!/usr/bin/env python3
"""Conductor — cdk8s generator.

Renders the *deployable* Conductor stack (Deployments, Services, ConfigMaps,
Fission CRs, RBAC, per-model head/worker templates, Tailscale Service) from a
single declarative source of truth. This is what Argo CD runs via the
`cdk8s-v1.0` CMP plugin for ONE homelab Application: the generator here decides
how every dependency is assembled, and Argo CD just applies the result.

No operator, no Helm chart, no hand-written Kustomize for the deployables.
"""

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


class ConductorStack(Chart):
    """Renders the whole self-contained Conductor app in one namespace."""

    def __init__(self, scope: Construct, cfg: dict, deploy: dict):
        namespace = deploy.get("namespace", "llm-test")
        image = deploy.get("image", "ghcr.io/projectinitiative/conductor:0.1.0")
        profiles = cfg.get("modelProfiles", {})

        super().__init__(scope, namespace, disable_resource_name_hashes=True)
        print(f"Rendering Conductor stack in namespace {namespace}", file=sys.stderr)

        self._namespace(namespace)
        self._crds()
        self._rbac(namespace)
        self._controller(namespace, image)

        # The Kubernetes-native operator starts with reference-only ownership
        # of explicitly declared workloads. The legacy router/Fission/model
        # templates are intentionally not rendered: they describe obsolete
        # vLLM lanes and would compete with homelab-owned TensorFold resources.
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
                 "resources": ["resourcepools/status", "managedworkloads/status", "workloadoverrides/status"],
                 "verbs": ["get", "update", "patch"]},
                {"apiGroups": ["apps"], "resources": ["deployments", "replicasets"],
                 "verbs": ["get", "list", "watch"]},
                {"apiGroups": ["apps"], "resources": ["deployments/scale"],
                 "verbs": ["get", "patch", "update"]},
                {"apiGroups": [""], "resources": ["pods", "services"],
                 "verbs": ["get", "list", "watch"]},
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

    # ---- legacy scheduler (not rendered) -------------------------------
    def _scheduler(self, ns, image):
        self._emit("conductor-deploy", "apps/v1", "Deployment", {
            "metadata": {"name": "conductor", "namespace": ns},
            "spec": {
                "replicas": 1,
                "strategy": {"type": "Recreate"},
                "selector": {"matchLabels": {"app": "conductor"}},
                "template": {
                    "metadata": {"labels": {"app": "conductor"}},
                    "spec": {
                        "serviceAccountName": "conductor",
                        "containers": [{
                            "name": "conductor",
                            "image": image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["/bin/conductor", "server"],
                            "ports": [{"name": "http", "containerPort": 8000}],
                            "readinessProbe": {"httpGet": {"path": "/health", "port": 8000},
                                               "initialDelaySeconds": 5, "periodSeconds": 10},
                            "livenessProbe": {"httpGet": {"path": "/health", "port": 8000},
                                              "initialDelaySeconds": 15, "periodSeconds": 20},
                            "resources": {"requests": {"cpu": "250m", "memory": "256Mi"},
                                          "limits": {"cpu": "1", "memory": "512Mi"}},
                            "volumeMounts": [
                                {"name": "state", "mountPath": "/etc/conductor/state", "readOnly": True},
                                {"name": "profiles", "mountPath": "/etc/conductor/profiles", "readOnly": True},
                            ],
                        }],
                        "volumes": [
                            {"name": "state", "configMap": {"name": "conductor-state"}},
                            {"name": "profiles", "configMap": {"name": "conductor-model-profiles"}},
                        ],
                    },
                },
            },
        })
        self._emit("conductor-svc", "v1", "Service", {
            "metadata": {"name": "conductor", "namespace": ns},
            "spec": {
                "selector": {"app": "conductor"},
                "ports": [{"name": "http", "port": 8000, "targetPort": 8000}],
            },
        })

    # ---- router (LiteLLM) ------------------------------------------------
    def _router(self, ns):
        self._emit("litellm-config", "v1", "ConfigMap", {
            "metadata": {"name": "litellm-config", "namespace": ns},
            "data": {"config.yaml": self._litellm_config()},
        })
        self._emit("litellm-deploy", "apps/v1", "Deployment", {
            "metadata": {"name": "litellm", "namespace": ns},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "litellm"}},
                "template": {
                    "metadata": {"labels": {"app": "litellm"}},
                    "spec": {
                        "containers": [{
                            "name": "litellm",
                            "image": "ghcr.io/berriai/litellm:main-latest",
                            "imagePullPolicy": "IfNotPresent",
                            "ports": [{"name": "http", "containerPort": 8000}],
                            "volumeMounts": [{"name": "config",
                                              "mountPath": "/app/config.yaml",
                                              "subPath": "config.yaml"}],
                        }],
                        "volumes": [{"name": "config",
                                     "configMap": {"name": "litellm-config"}}],
                    },
                },
            },
        })
        self._emit("litellm-svc", "v1", "Service", {
            "metadata": {"name": "litellm", "namespace": ns},
            "spec": {
                "selector": {"app": "litellm"},
                "ports": [{"name": "http", "port": 8000, "targetPort": 8000}],
            },
        })

    def _litellm_config(self):
        entries = "\n".join(
            f"      - model_name: {name}\n"
            f"        litellm_params:\n"
            f"          model: hosted_vllm/{name}\n"
            f"          api_base: http://conductor:8000/v1\n"
            f"          api_key: os.environ/VLLM_API_KEY\n"
            for name in ["deepseek-v4-flash-vision-exp", "GLM-5.3-Flash-EXL3", "qwen3.8-flash-next"]
        )
        return ("model_list:\n" + entries +
                "general_settings:\n  master_key: os.environ/LITELLM_MASTER_KEY\n")

    # ---- Fission ---------------------------------------------------------
    def _fission(self, ns, image):
        self._emit("fission-env-conductor", "fission.io/v1", "Environment", {
            "metadata": {"name": "conductor", "namespace": ns},
            "spec": {
                "version": 3,
                "runtime": {"image": image,
                            "container": {"command": ["/bin/conductor", "function"]},
                            "podSpec": {"serviceAccountName": "conductor"}},
                "defaultPoolSize": 1,
                "allowedFunctionsPerPod": 1,
            },
        })
        for fn in ["conductor-status", "conductor-model-ready"]:
            self._emit("fission-fn-" + fn, "fission.io/v1", "Function", {
                "metadata": {"name": fn, "namespace": ns},
                "spec": {
                    "environment": "conductor",
                    "invokeStrategy": {"strategy": "newdeploy",
                                       "executionStrategy": {"minScale": 1, "maxScale": 1}},
                },
            })
        self._emit("fission-http-status", "fission.io/v1", "HTTPTrigger", {
            "metadata": {"name": "conductor-status", "namespace": ns},
            "spec": {
                "relativeURL": "/conductor/status", "method": "GET",
                "functionref": {"name": "conductor-status", "type": "function"},
            },
        })
        self._emit("fission-watch-ready", "fission.io/v1", "KubernetesWatchTrigger", {
            "metadata": {"name": "conductor-model-ready", "namespace": ns},
            "spec": {
                "namespace": ns, "type": "deployment",
                "labelSelector": "conductor.io/role=head",
                "functionref": {"name": "conductor-model-ready", "type": "function"},
            },
        })

    # ---- models ----------------------------------------------------------
    def _models(self, ns, profiles):
        self._emit("model-cache", "v1", "PersistentVolumeClaim", {
            "metadata": {"name": "model-cache", "namespace": ns},
            "spec": {
                "accessModes": ["ReadWriteMany"],
                "storageClassName": "juicefs-sc",
                "resources": {"requests": {"storage": "1Ti"}},
            },
        })
        for name, p in profiles.items():
            self._model_pair(ns, name, p)

    def _model_pair(self, ns, name, p):
        head = p.get("head", {})
        worker = p.get("worker", {})
        port = head.get("port", 8000)
        image = p.get("image", "vllm/vllm-openai:latest")
        head_name = head.get("deployment", name.lower() + "-head")
        worker_name = worker.get("deployment", name.lower() + "-worker")

        self._emit("head-" + name.lower(), "apps/v1", "Deployment", {
            "metadata": {"name": head_name, "namespace": ns},
            "spec": self._model_deploy_spec(
                name, image, port, head_name, role="head",
                labels={"app": name.lower(), "conductor.io/model": name, "conductor.io/role": "head"},
            ),
        })
        self._emit("worker-" + name.lower(), "apps/v1", "Deployment", {
            "metadata": {"name": worker_name, "namespace": ns},
            "spec": self._model_deploy_spec(
                name, image, port, worker_name, role="worker",
                labels={"app": name.lower(), "conductor.io/model": name, "conductor.io/role": "worker"},
            ),
        })
        self._emit("svc-" + name.lower() + "-head", "v1", "Service", {
            "metadata": {"name": name.lower() + "-head", "namespace": ns},
            "spec": {
                "selector": {"conductor.io/role": "head", "conductor.io/model": name},
                "ports": [{"name": "http", "port": port, "targetPort": port}],
            },
        })

    def _model_deploy_spec(self, name, image, port, deploy, role, labels):
        node = "chronometer" if role == "head" else "sextant"
        cmd = self._head_cmd(name, port) if role == "head" else self._worker_cmd(name, port)
        return {
            "replicas": 0,
            "strategy": {"type": "Recreate"},
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "hostNetwork": True,
                    "hostIPC": True,
                    "dnsPolicy": "ClusterFirstWithHostNet",
                    "nodeSelector": {"kubernetes.io/hostname": node},
                    "containers": [{
                        "name": "vllm",
                        "image": image,
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["/bin/bash", "-lc"],
                        "args": [cmd],
                        "ports": [{"name": "http", "containerPort": port}],
                        "volumeMounts": [{"name": "models", "mountPath": "/models"}],
                    }],
                    "volumes": [{"name": "models",
                                 "persistentVolumeClaim": {"claimName": "model-cache"}}],
                },
            },
        }

    def _head_cmd(self, name, port):
        # Structural rank-0 head. Model-specific patches/env are added per model
        # (see DESIGN.md); this is the multi-node vLLM skeleton.
        return ("set -euxo pipefail\n"
                f"exec /usr/bin/vllm serve {name} --served-model-name {name} "
                "--tensor-parallel-size 2 --nnodes 2 --node-rank 0 "
                "--distributed-executor-backend mp --host 0.0.0.0 "
                f"--port {port}\n")

    def _worker_cmd(self, name, port):
        # Structural rank-1 headless worker.
        return ("set -euxo pipefail\n"
                f"exec /usr/bin/vllm serve {name} --served-model-name {name} "
                "--tensor-parallel-size 2 --nnodes 2 --node-rank 1 --headless "
                "--distributed-executor-backend mp --host 0.0.0.0 "
                f"--port {port}\n")

    # ---- services ---------------------------------------------------------
    def _services(self, ns):
        self._emit("ai-svc", "v1", "Service", {
            "metadata": {"name": "ai", "namespace": ns,
                         "annotations": {"tailscale.com/hostname": "ai",
                                         "tailscale.com/proxy-group": "mc-ingress-proxy"}},
            "spec": {
                "type": "LoadBalancer",
                "loadBalancerClass": "tailscale",
                "selector": {"app": "litellm"},
                "ports": [{"name": "http", "protocol": "TCP", "port": 80, "targetPort": 8000}],
            },
        })

    # ---- state / model-profiles ------------------------------------------
    def _state_configmaps(self, ns, profiles):
        self._emit("conductor-state", "v1", "ConfigMap", {
            "metadata": {"name": "conductor-state", "namespace": ns},
            "data": {"state.yaml": "resident: \"\"\ndesired: \"\"\nstatus: idle\nstarted_at: \"\"\n"},
        })
        self._emit("conductor-model-profiles", "v1", "ConfigMap", {
            "metadata": {"name": "conductor-model-profiles", "namespace": ns},
            "data": {"models.yaml": self._profiles_yaml(profiles)},
        })

    def _profiles_yaml(self, profiles):
        out = ["models:"]
        for name, p in profiles.items():
            head = p.get("head", {})
            worker = p.get("worker", {})
            health = p.get("health", {})
            quantum = p.get("quantum", {})
            out.append(f"  - name: {name}")
            out.append(f"    priority: {p.get('priority', 9)}")
            out.append(f"    image: {p.get('image', '')}")
            out.append(f"    head: {{deployment: {head.get('deployment', '')}, "
                       f"hostIP: {head.get('hostIP', '')}, port: {head.get('port', 8000)}}}")
            out.append(f"    worker: {{deployment: {worker.get('deployment', '')}, "
                       f"hostIP: {worker.get('hostIP', '')}}}")
            out.append(f"    health: {{path: {health.get('path', '/health')}, "
                       f"timeout: {health.get('timeout', '3600s')}}}")
            out.append(f"    quantum: {{max_requests: {quantum.get('max_requests', 16)}, "
                       f"max_seconds: {quantum.get('max_seconds', 600)}}}")
            out.append(f"    min_dwell: {p.get('min_dwell', '900s')}")
            out.append(f"    cooldown: {p.get('cooldown', '300s')}")
        return "\n".join(out) + "\n"


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

    app = App(outdir=os.environ.get("MANIFEST_OUTPUT_DIR", ".direnv/manifests"))
    for cluster_file in cluster_files:
        cluster = load_yaml(cluster_file) or {}
        for deploy in cluster.get("apps", []):
            ConductorStack(app, cfg, deploy)
    app.synth()
    print("Conductor deployable rendered.", file=sys.stderr)


if __name__ == "__main__":
    main()
