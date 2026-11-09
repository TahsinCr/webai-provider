"""Web response normalization, isolated from transport and application state."""

from typing import Any

from .errors import ChatGPTError


def page(raw: Any) -> tuple[list[dict[str, Any]], str | None]:
    if not isinstance(raw, dict) or not isinstance(raw.get("items"), list):
        raise ChatGPTError("Project response changed: expected items list")
    if any(not isinstance(item, dict) for item in raw["items"]):
        raise ChatGPTError("Project response contains an invalid item")
    cursor = raw.get("cursor") or raw.get("next_cursor")
    if cursor is not None and (not isinstance(cursor, str) or not cursor):
        raise ChatGPTError("Project response contains an invalid cursor")
    return raw["items"], cursor


def project(raw: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        raise ChatGPTError("Project response changed: expected an object")
    envelope = raw
    gizmo = raw
    for _ in range(3):
        nested = gizmo.get("gizmo")
        if not isinstance(nested, dict):
            break
        envelope, gizmo = gizmo, nested
    project_id = gizmo.get("id")
    if not isinstance(project_id, str) or not project_id:
        raise ChatGPTError("Project response contains a project without an ID")
    if not project_id.startswith("g-p-") and gizmo.get("gizmo_type") != "snorlax":
        return None
    display = gizmo.get("display") or {}
    if not isinstance(display, dict):
        raise ChatGPTError("Project display response changed")
    files = envelope.get("files", gizmo.get("files", []))
    if not isinstance(files, list) or any(not isinstance(f, dict) for f in files):
        raise ChatGPTError("Project resource response changed")
    return {
        "id": project_id,
        "name": str(display.get("name") or gizmo.get("name") or project_id),
        "description": display.get("description") or "",
        "instructions": gizmo.get("instructions") or "",
        "resources": files,
        "metadata": dict(raw),
    }


def project_id(raw: dict[str, Any]) -> str | None:
    value = raw.get("project_id") or raw.get("gizmo_id") or raw.get("conversation_template_id")
    return value if isinstance(value, str) and value.startswith("g-p-") else None
