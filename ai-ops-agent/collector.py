"""
Collector: given an incident dict from the Detector, gathers all context
needed for root-cause analysis:
  - Current + previous container logs (previous matters most for
    CrashLoopBackOff - the crash reason is usually in the log right
    before the restart, not the fresh empty log of the new attempt)
  - Recent Kubernetes Events involving the pod (scheduling failures,
    probe failures, pull errors, etc.)
  - Pod spec/status summary (equivalent to `kubectl describe`'s key
    fields: resource requests/limits, conditions, image, node)
  - Resource usage at time of incident, via the metrics-server API
    (helps confirm/rule out OOM and CPU throttling as root causes)

Returns a single "context bundle" dict that the Orchestrator (Phase 5)
turns into a prompt for the LLM.
"""

import logging
from kubernetes import client
from kubernetes.client.rest import ApiException

log = logging.getLogger("collector")

LOG_TAIL_LINES = 200


def _safe_call(fn, default=None, **kwargs):
    try:
        return fn(**kwargs)
    except ApiException as e:
        log.debug("K8s API call failed (%s): %s", fn.__name__, e.reason)
        return default
    except Exception:
        log.exception("Unexpected error calling %s", getattr(fn, "__name__", fn))
        return default


def get_logs(v1: client.CoreV1Api, namespace, pod, container, previous=False):
    return _safe_call(
        v1.read_namespaced_pod_log,
        default=None,
        name=pod,
        namespace=namespace,
        container=container,
        previous=previous,
        tail_lines=LOG_TAIL_LINES,
    )


def get_events(v1: client.CoreV1Api, namespace, pod_name):
    """Recent Events where this pod is the involved object, newest first."""
    field_selector = f"involvedObject.name={pod_name}"
    events = _safe_call(
        v1.list_namespaced_event,
        default=None,
        namespace=namespace,
        field_selector=field_selector,
    )
    if not events:
        return []
    items = sorted(events.items, key=lambda e: e.last_timestamp or e.event_time or "", reverse=True)
    return [
        {
            "type": e.type,
            "reason": e.reason,
            "message": e.message,
            "count": e.count,
            "last_seen": str(e.last_timestamp or e.event_time),
        }
        for e in items[:20]
    ]


def get_pod_summary(v1: client.CoreV1Api, namespace, pod_name):
    pod = _safe_call(v1.read_namespaced_pod, default=None, name=pod_name, namespace=namespace)
    if not pod:
        return None

    containers_spec = []
    for c in pod.spec.containers:
        containers_spec.append({
            "name": c.name,
            "image": c.image,
            "resources": {
                "requests": (c.resources.requests or {}) if c.resources else {},
                "limits": (c.resources.limits or {}) if c.resources else {},
            },
        })

    conditions = [
        {"type": c.type, "status": c.status, "reason": c.reason, "message": c.message}
        for c in (pod.status.conditions or [])
    ]

    return {
        "node": pod.spec.node_name,
        "phase": pod.status.phase,
        "start_time": str(pod.status.start_time),
        "containers": containers_spec,
        "conditions": conditions,
        "labels": pod.metadata.labels,
    }


def get_resource_usage(custom_api: client.CustomObjectsApi, namespace, pod_name):
    """
    Live usage from metrics-server (metrics.k8s.io). Requires metrics-server
    installed in the cluster. Returns None gracefully if unavailable so the
    rest of the collector still works without it.
    """
    result = _safe_call(
        custom_api.get_namespaced_custom_object,
        default=None,
        group="metrics.k8s.io",
        version="v1beta1",
        namespace=namespace,
        plural="pods",
        name=pod_name,
    )
    if not result:
        return None

    return [
        {"container": c["name"], "cpu": c["usage"]["cpu"], "memory": c["usage"]["memory"]}
        for c in result.get("containers", [])
    ]


def gather_context(incident: dict) -> dict:
    """
    Main entry point. Takes a Detector incident dict and returns a full
    context bundle ready for the RCA orchestrator.
    """
    v1 = client.CoreV1Api()
    custom_api = client.CustomObjectsApi()

    namespace = incident.get("namespace")
    pod_name = incident.get("pod")
    container_name = incident.get("container")

    context = {
        "incident": incident,
        "pod_summary": None,
        "events": [],
        "logs_current": None,
        "logs_previous": None,
        "resource_usage": None,
    }

    if incident.get("type") == "deployment_stuck":
        # Deployment-level incidents don't have a single pod - collect
        # events scoped to the deployment name instead.
        context["events"] = get_events(v1, namespace, incident.get("deployment"))
        return context

    if not namespace or not pod_name:
        log.warning("Incident missing namespace/pod, cannot collect context: %s", incident)
        return context

    context["pod_summary"] = get_pod_summary(v1, namespace, pod_name)
    context["events"] = get_events(v1, namespace, pod_name)
    context["resource_usage"] = get_resource_usage(custom_api, namespace, pod_name)

    if container_name:
        context["logs_current"] = get_logs(v1, namespace, pod_name, container_name, previous=False)
        context["logs_previous"] = get_logs(v1, namespace, pod_name, container_name, previous=True)
    elif context["pod_summary"] and context["pod_summary"]["containers"]:
        # Fall back to the first container if the incident didn't specify one
        first_container = context["pod_summary"]["containers"][0]["name"]
        context["logs_current"] = get_logs(v1, namespace, pod_name, first_container, previous=False)
        context["logs_previous"] = get_logs(v1, namespace, pod_name, first_container, previous=True)

    return context
