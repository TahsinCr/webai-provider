"""Grok conversation and workspace records mapped to the shared contract."""

from ..api import MessageAction
from ..messages import sources_from
from ..models import Content, Conversation, Message, Origin, Project, Resource


def _text(value):
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "\n".join(
            part
            for item in value
            if isinstance(item, dict)
            for part in (item.get("text") or item.get("summary") or item.get("message"),)
            if isinstance(part, str)
        )
    return ""


def active_responses(history):
    rows = (
        history.get("responses", history.get("responseNodes", []))
        if isinstance(history, dict)
        else []
    )
    if not isinstance(rows, list):
        return ()
    rows = [row for row in rows if isinstance(row, dict) and row.get("responseId")]
    by_id = {row["responseId"]: row for row in rows}
    container = history.get("conversation", {}) if isinstance(history, dict) else {}
    leaf_id = (
        history.get("lastResponseId")
        or history.get("currentLeafResponseId")
        or (container.get("lastResponseId") if isinstance(container, dict) else None)
    )
    if leaf_id not in by_id:
        parents = {row.get("parentResponseId") for row in rows}
        leaves = [row for row in rows if row.get("responseId") not in parents]
        leaf_id = leaves[-1].get("responseId") if leaves else None
    if leaf_id in by_id:
        branch, seen = [], set()
        while leaf_id in by_id and leaf_id not in seen:
            seen.add(leaf_id)
            row = by_id[leaf_id]
            branch.append(row)
            leaf_id = row.get("parentResponseId")
        rows = list(reversed(branch))

    return tuple(rows)


def _messages(history):
    rows = active_responses(history)
    messages = []
    for row in rows:
        response_id = row["responseId"]
        sender = str(row.get("sender", "")).lower()
        if sender in ("human", "user"):
            text = row.get("message") or row.get("query") or ""
            messages.append(Message(response_id, "user", (Content("text", str(text)),)))
            continue
        if sender not in ("assistant", "model", "grok"):
            continue
        thinking = _text(row.get("thinkingTrace"))
        if thinking:
            messages.append(
                Message(
                    response_id + ":analysis",
                    "assistant",
                    (Content("text", thinking),),
                    "analysis",
                    {"native_response_id": response_id},
                )
            )
        answer = row.get("message") or ""
        if not isinstance(answer, str):
            answer = _text(answer)
        sources = sources_from({"sources": row.get("webSearchResults", [])})
        messages.append(
            Message(
                response_id,
                "assistant",
                (Content("text", answer),),
                "final",
                {
                    "model": row.get("model"),
                    "sources": sources,
                    "native_response_id": response_id,
                },
                (MessageAction.RETRY.value,) if answer else (),
            )
        )
    return tuple(messages)


def conversation(value, account_id, history=None):
    if not isinstance(value, dict):
        raise ValueError("Grok conversation response contract changed")
    identifier = value.get("conversationId") or value.get("id")
    if not isinstance(identifier, str) or not identifier:
        raise ValueError("Grok conversation response contract changed")
    workspaces = value.get("workspaces") or ()
    project_id = value.get("workspaceId")
    if not project_id and workspaces and isinstance(workspaces[0], dict):
        project_id = workspaces[0].get("workspaceId") or workspaces[0].get("id")
    return Conversation(
        Origin("grok", account_id, identifier),
        value.get("title") or "Grok conversation",
        _messages(history) if history is not None else (),
        project_id,
        {
            "temporary": bool(value.get("temporary")),
            "created_at": value.get("createTime"),
            "modified_at": value.get("modifyTime"),
        },
    )


def project(value, account_id, files=()):
    if not isinstance(value, dict):
        raise ValueError("Grok workspace response contract changed")
    identifier = value.get("workspaceId") or value.get("id")
    if not isinstance(identifier, str) or not identifier:
        raise ValueError("Grok workspace response contract changed")
    resources = tuple(
        Resource(
            str(item.get("assetId") or item.get("path") or item.get("name")),
            str(item.get("name") or item.get("path") or "Source"),
            item.get("mimeType"),
            metadata={
                key: item[key]
                for key in ("path", "assetId", "size", "createdAt", "modifiedAt")
                if item.get(key) is not None
            },
        )
        for item in files
        if isinstance(item, dict)
        and not item.get("isDirectory")
        and (item.get("assetId") or item.get("path"))
    )
    return Project(
        Origin("grok", account_id, identifier),
        str(value.get("name") or "Grok project"),
        value.get("customPersonality"),
        resources,
        {"preferred_model": value.get("preferredModel"), "kind": value.get("kind")},
    )
