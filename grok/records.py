"""Grok conversation and workspace records mapped to the shared contract."""

import re

from ..api import MessageAction
from ..errors import ProviderError
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


class GrokTextFilter:
    _TOOL_OPEN = "<xai:tool_usage_card"
    _TOOL_CLOSE = "</xai:tool_usage_card>"
    _RENDER_OPEN = "<grok:render"
    _RENDER_CLOSE = "</grok:render>"
    _MARKERS = (_TOOL_OPEN, _RENDER_OPEN, _RENDER_CLOSE)
    _MARKUP = re.compile(r"<xai:tool_usage_card\b|</?grok:render\b")

    def __init__(self):
        self._pending = ""
        self._inside_tool_card = False
        self._started = False

    def _visible(self, text):
        if not self._started:
            text = text.lstrip()
            self._started = bool(text)
        return text

    @classmethod
    def _safe_end(cls, text):
        markers = (*cls._MARKERS, cls._TOOL_CLOSE)
        for size in range(min(len(text), max(map(len, markers))), 0, -1):
            if any(marker.startswith(text[-size:]) for marker in markers):
                return len(text) - size
        return len(text)

    def feed(self, text="", *, final=False):
        self._pending += text
        output = []
        while self._pending:
            if self._inside_tool_card:
                end = self._pending.find(self._TOOL_CLOSE)
                if end < 0:
                    safe_end = self._safe_end(self._pending)
                    self._pending = "" if final else self._pending[safe_end:]
                    break
                self._pending = self._pending[end + len(self._TOOL_CLOSE) :]
                self._inside_tool_card = False
                continue

            match = self._MARKUP.search(self._pending)
            if match is None:
                safe_end = self._safe_end(self._pending)
                output.append(self._visible(self._pending[:safe_end]))
                self._pending = "" if final else self._pending[safe_end:]
                break

            index = match.start()
            output.append(self._visible(self._pending[:index]))
            self._pending = self._pending[index:]
            end = self._pending.find(">")
            if end < 0:
                if final:
                    self._pending = ""
                elif len(self._pending) > 4096:
                    output.append(self._visible(self._pending[:1]))
                    self._pending = self._pending[1:]
                    continue
                break
            marker = self._pending[:end]
            self._pending = self._pending[end + 1 :]
            self._inside_tool_card = marker.startswith(self._TOOL_OPEN)
        return "".join(output)


def clean_text(value):
    return GrokTextFilter().feed(_text(value), final=True)


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
        thinking = clean_text(row.get("thinkingTrace"))
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
        answer = clean_text(row.get("message"))
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
        raise ProviderError("Grok conversation response contract changed")
    identifier = value.get("conversationId") or value.get("id")
    if not isinstance(identifier, str) or not identifier:
        raise ProviderError("Grok conversation response contract changed")
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
        raise ProviderError("Grok workspace response contract changed")
    identifier = value.get("workspaceId") or value.get("id")
    if not isinstance(identifier, str) or not identifier:
        raise ProviderError("Grok workspace response contract changed")
    resources = tuple(
        Resource(
            str(item.get("path") or item.get("assetId")),
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
