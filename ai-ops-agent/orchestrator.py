"""
Orchestrator: takes a context bundle from the Collector, builds a
root-cause-analysis prompt, sends it to the in-cluster Ollama service,
and returns a structured incident report.

Design notes:
  - Talks to Ollama over its HTTP API (not the ollama CLI) since this
    runs as a long-lived service making many requests.
  - Truncates logs/events to keep the prompt within a reasonable context
    window - Phi-3.5 has a 128k window in theory, but small CPU-served
    models get noticeably slower and less reliable well before that, so
    we keep prompts tight and focused.
  - Asks the model for a strict JSON shape so the Dashboard (Phase 6)
    can render it without fragile text parsing. Small models don't
    always obey this perfectly, so we validate and fall back to raw
    text if JSON parsing fails, rather than crashing the pipeline.
  - This is READ-ONLY / advisory by design: the model is explicitly
    instructed to only ever suggest commands, never claim to have run
    them. No command from here is ever auto-executed.
"""

import json
import logging
import os
import re

import requests

log = logging.getLogger("orchestrator")

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://ollama.ai-ops-agent.svc.cluster.local:11434")
MODEL_NAME = os.environ.get("MODEL_NAME", "phi3.5:3.8b")
REQUEST_TIMEOUT_SECONDS = int(os.environ.get("LLM_TIMEOUT_SECONDS", "240"))

MAX_LOG_CHARS = 3000     # per log field (current/previous)
MAX_EVENTS = 10

SYSTEM_PROMPT = """You are a Kubernetes site-reliability expert analyzing a cluster incident.
You are advisory only: you NEVER claim to have executed a command, only recommend one.
Respond with ONLY a JSON object, no other text, no markdown fences, matching exactly this shape:
{
  "summary": "one sentence describing what went wrong",
  "root_cause": "your best assessment of the root cause, 2-4 sentences",
  "confidence": "high|medium|low",
  "evidence": ["short bullet citing specific log/event lines that support the root cause"],
  "recommended_commands": ["kubectl ... commands the operator should run, in order"],
  "prevention": "one short suggestion to prevent recurrence"
}"""


def _truncate(text, max_chars):
    if not text:
        return text
    if len(text) <= max_chars:
        return text
    return "...(truncated)...\n" + text[-max_chars:]


def build_prompt(context: dict) -> str:
    incident = context.get("incident", {})
    pod_summary = context.get("pod_summary")
    events = context.get("events", [])[:MAX_EVENTS]
    logs_current = _truncate(context.get("logs_current"), MAX_LOG_CHARS)
    logs_previous = _truncate(context.get("logs_previous"), MAX_LOG_CHARS)
    resource_usage = context.get("resource_usage")

    parts = [f"## Incident\n{json.dumps(incident, indent=2)}"]

    if pod_summary:
        parts.append(f"## Pod spec/status\n{json.dumps(pod_summary, indent=2)}")

    if events:
        parts.append(f"## Recent Kubernetes Events (newest first)\n{json.dumps(events, indent=2)}")

    if resource_usage:
        parts.append(f"## Current resource usage\n{json.dumps(resource_usage, indent=2)}")

    if logs_previous:
        parts.append(f"## Previous container logs (before last restart, often has the crash reason)\n{logs_previous}")

    if logs_current:
        parts.append(f"## Current container logs\n{logs_current}")

    return "\n\n".join(parts)


def _extract_json(text: str) -> dict | None:
    """
    Small models sometimes wrap JSON in markdown fences or add stray text.
    Try direct parse first, then fall back to extracting the first {...} block.
    """
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    return None


def analyze(context: dict) -> dict:
    """
    Sends the context bundle to the LLM and returns a structured report.
    On any failure (timeout, malformed JSON, connection error), returns
    a degraded-but-still-useful report rather than raising, so a bad LLM
    response never crashes the detector pipeline.
    """
    prompt = build_prompt(context)

    payload = {
        "model": MODEL_NAME,
        "system": SYSTEM_PROMPT,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "keep_alive": -1,   # keep model resident indefinitely - avoids slow cold reloads between incidents
        "options": {
            "temperature": 0.2,   # low temperature: we want consistent, factual RCA, not creative variation
            "num_predict": 600,
        },
    }

    try:
        resp = requests.post(f"{OLLAMA_URL}/api/generate", json=payload, timeout=REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        raw_output = resp.json().get("response", "")
    except requests.exceptions.RequestException as e:
        log.error("LLM request failed: %s", e)
        return _fallback_report(context, error=str(e))

    parsed = _extract_json(raw_output)
    if parsed is None:
        log.warning("LLM did not return valid JSON, falling back to raw text. Output: %s", raw_output[:500])
        return _fallback_report(context, raw_text=raw_output)

    parsed.setdefault("summary", "No summary provided by model")
    parsed.setdefault("root_cause", "Unknown")
    parsed.setdefault("confidence", "low")
    parsed.setdefault("evidence", [])
    parsed.setdefault("recommended_commands", [])
    parsed.setdefault("prevention", "")
    parsed["incident"] = context.get("incident")
    return parsed


def _fallback_report(context, error=None, raw_text=None):
    incident = context.get("incident", {})
    return {
        "summary": f"Automated RCA unavailable for {incident.get('pod') or incident.get('deployment')}",
        "root_cause": raw_text or f"LLM analysis failed: {error}" if error else "Could not parse model output",
        "confidence": "low",
        "evidence": [],
        "recommended_commands": [
            f"kubectl describe pod {incident.get('pod')} -n {incident.get('namespace')}",
            f"kubectl logs {incident.get('pod')} -n {incident.get('namespace')} --previous",
        ] if incident.get("pod") else [],
        "prevention": "",
        "incident": incident,
    }
