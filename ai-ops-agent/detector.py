"""
Detector: watches the Kubernetes API for pod and deployment failures and
fires an incident event when it finds one.

Detects:
  - CrashLoopBackOff
  - OOMKilled containers
  - Pod phase == Failed
  - ImagePullBackOff / ErrImagePull
  - Deployment rollouts that are stuck / not progressing

Design notes:
  - Uses the K8s watch API (long-lived connection) rather than polling, so
    detection is near-real-time and cheap on the API server.
  - Watch connections drop periodically (this is normal K8s behavior) -
    the outer loop reconnects automatically.
  - De-duplicates: a crash-looping pod fires events repeatedly as its
    restart count climbs. We only fire once per (pod, reason) until the
    pod is deleted or recovers, tracked in `_seen`.
  - `handle_incident()` is the integration point for Phase 4 (Collector).
    For now it logs a structured incident record; next phase will pass
    it to the collector to gather logs/events/metrics.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

from kubernetes import client, config, watch

import collector
import orchestrator

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("detector")

# Namespaces to watch. "" (empty) with watch_pod_for_all_namespaces means
# cluster-wide. Set WATCH_NAMESPACE env var to restrict to one namespace.
WATCH_NAMESPACE = os.environ.get("WATCH_NAMESPACE", "")

# Track incidents we've already fired, so we don't spam on every restart
# tick of an already-known crash loop. Cleared when the pod disappears.
_seen = set()


def load_k8s_config():
    try:
        config.load_incluster_config()
        log.info("Loaded in-cluster kubeconfig")
    except config.ConfigException:
        config.load_kube_config()
        log.info("Loaded local kubeconfig (dev mode)")


def handle_incident(incident: dict):
    """
    Fired whenever the watchers detect a new failure. Gathers full
    context via the Collector; the RCA orchestrator (Phase 5) will
    consume this next.
    """
    log.warning("INCIDENT DETECTED: %s", incident)
    context = collector.gather_context(incident)
    log.info(
        "Context gathered for %s/%s: events=%d logs_current=%s logs_previous=%s resource_usage=%s",
        incident.get("namespace"), incident.get("pod") or incident.get("deployment"),
        len(context["events"]),
        bool(context["logs_current"]),
        bool(context["logs_previous"]),
        context["resource_usage"],
    )

    report = orchestrator.analyze(context)
    log.warning("RCA REPORT: %s", json.dumps(report, indent=2))
    # TODO(phase 6): push `report` to the dashboard instead of just logging it


def _dedup_key(namespace, name, reason):
    return f"{namespace}/{name}/{reason}"


def _mark_seen(namespace, name, reason):
    _seen.add(_dedup_key(namespace, name, reason))


def _already_seen(namespace, name, reason):
    return _dedup_key(namespace, name, reason) in _seen


def _clear_seen_for_pod(namespace, name):
    _seen.difference_update({k for k in _seen if k.startswith(f"{namespace}/{name}/")})


def inspect_pod(pod) -> list:
    """Return a list of incident dicts for a given pod's current status."""
    incidents = []
    namespace = pod.metadata.namespace
    name = pod.metadata.name
    phase = pod.status.phase

    # Pod-level Failed phase
    if phase == "Failed":
        reason = pod.status.reason or "PodFailed"
        if not _already_seen(namespace, name, reason):
            incidents.append({
                "type": "pod_failed",
                "namespace": namespace,
                "pod": name,
                "reason": reason,
                "message": pod.status.message,
                "detected_at": datetime.now(timezone.utc).isoformat(),
            })
            _mark_seen(namespace, name, reason)

    # Container-level issues: waiting reasons (CrashLoopBackOff,
    # ImagePullBackOff, ErrImagePull) and terminated reasons (OOMKilled,
    # Error with non-zero exit code)
    statuses = (pod.status.container_statuses or []) + (pod.status.init_container_statuses or [])
    for cs in statuses:
        container_name = cs.name
        restart_count = cs.restart_count

        waiting = cs.state.waiting
        if waiting and waiting.reason in (
            "CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull"
        ):
            reason = waiting.reason
            if not _already_seen(namespace, name, reason):
                incidents.append({
                    "type": "container_waiting",
                    "namespace": namespace,
                    "pod": name,
                    "container": container_name,
                    "reason": reason,
                    "message": waiting.message,
                    "restart_count": restart_count,
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                })
                _mark_seen(namespace, name, reason)

        terminated = cs.state.terminated
        if terminated and terminated.reason in ("OOMKilled", "Error"):
            reason = terminated.reason
            if not _already_seen(namespace, name, reason):
                incidents.append({
                    "type": "container_terminated",
                    "namespace": namespace,
                    "pod": name,
                    "container": container_name,
                    "reason": reason,
                    "exit_code": terminated.exit_code,
                    "restart_count": restart_count,
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                })
                _mark_seen(namespace, name, reason)

    return incidents


def watch_pods(v1: client.CoreV1Api):
    w = watch.Watch()
    log.info("Watching pods (namespace=%s)", WATCH_NAMESPACE or "ALL")

    if WATCH_NAMESPACE:
        stream = w.stream(v1.list_namespaced_pod, namespace=WATCH_NAMESPACE, timeout_seconds=300)
    else:
        stream = w.stream(v1.list_pod_for_all_namespaces, timeout_seconds=300)

    for event in stream:
        pod = event["object"]
        event_type = event["type"]  # ADDED, MODIFIED, DELETED

        if event_type == "DELETED":
            _clear_seen_for_pod(pod.metadata.namespace, pod.metadata.name)
            continue

        try:
            for incident in inspect_pod(pod):
                handle_incident(incident)
        except Exception:
            log.exception("Error inspecting pod %s/%s", pod.metadata.namespace, pod.metadata.name)


def watch_deployments(apps_v1: client.AppsV1Api):
    """
    Detects deployments that are not progressing - e.g. a bad rollout
    where new pods never become Available. Checks the 'Progressing'
    condition for status=False (stuck) rather than just replica counts,
    since that's the authoritative signal Kubernetes itself uses.
    """
    w = watch.Watch()
    log.info("Watching deployments (namespace=%s)", WATCH_NAMESPACE or "ALL")

    if WATCH_NAMESPACE:
        stream = w.stream(apps_v1.list_namespaced_deployment, namespace=WATCH_NAMESPACE, timeout_seconds=300)
    else:
        stream = w.stream(apps_v1.list_deployment_for_all_namespaces, timeout_seconds=300)

    for event in stream:
        dep = event["object"]
        if event["type"] == "DELETED":
            continue

        namespace = dep.metadata.namespace
        name = dep.metadata.name
        conditions = dep.status.conditions or []

        for cond in conditions:
            if cond.type == "Progressing" and cond.status == "False":
                reason = cond.reason or "DeploymentStuck"
                if not _already_seen(namespace, name, reason):
                    handle_incident({
                        "type": "deployment_stuck",
                        "namespace": namespace,
                        "deployment": name,
                        "reason": reason,
                        "message": cond.message,
                        "detected_at": datetime.now(timezone.utc).isoformat(),
                    })
                    _mark_seen(namespace, name, reason)
            elif cond.type == "Progressing" and cond.status == "True":
                # Recovered - clear so a future stuck state fires again
                _clear_seen_for_pod(namespace, name)


def run_forever(target_fn, *args):
    """Wrap a watch function so a dropped connection just reconnects."""
    while True:
        try:
            target_fn(*args)
        except Exception:
            log.exception("Watch stream error, reconnecting in 5s")
            time.sleep(5)


if __name__ == "__main__":
    load_k8s_config()
    v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()

    import threading

    t1 = threading.Thread(target=run_forever, args=(watch_pods, v1), daemon=True)
    t2 = threading.Thread(target=run_forever, args=(watch_deployments, apps_v1), daemon=True)
    t1.start()
    t2.start()

    log.info("Detector running. Watching pods + deployments cluster-wide.")
    t1.join()
    t2.join()
