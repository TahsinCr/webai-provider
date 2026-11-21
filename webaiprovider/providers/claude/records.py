"""Lossless Claude records mapped to the shared provider contract."""

import re

from ...errors import ProviderError
from ...resources.chat.types import Conversation
from ...resources.message.attachment import Attachment
from ...resources.message.types import (
    Content,
    MessageAction,
    MessageData,
    actions_for,
    target_message,
)
from ...resources.project.types import ProjectData
from ...resources.response.artifact import FileTargetRule, files_from
from ...resources.response.source import sources_from
from .errors import ClaudeCompatibilityError

_CLAUDE_SANDBOX_RE = re.compile(
    r"\[([^\]\n]{1,200})\]\(((?:sandbox:|computer:)?/mnt/user-data/[^\s)]+)\)"
)

CLAUDE_FILE_RULE = FileTargetRule(
    parse_target=lambda _target, parsed: (
        parsed.scheme in ("", "sandbox", "computer")
        and parsed.path.startswith("/mnt/user-data/")
        and not parsed.netloc
    ),
    matches_identity=lambda identity, target: not target and identity.startswith("/mnt/user-data/"),
    markdown_link_re=_CLAUDE_SANDBOX_RE,
    markdown_hint="/mnt/user-data/",
)


def presented_files(block):
    """Only the provider's explicit presentation tool publishes generated output paths."""
    if block.get("type") == "tool_use" and block.get("name") == "present_files":
        inputs = block.get("input")
        paths = inputs.get("filepaths") or () if isinstance(inputs, dict) else ()
        return (
            files_from(
                {"files": [{"path": path} for path in paths[:128] if isinstance(path, str)]},
                rule=CLAUDE_FILE_RULE,
            )
            if isinstance(paths, (list, tuple))
            else ()
        )
    return files_from(block, rule=CLAUDE_FILE_RULE)


def _attachment_count(turns):
    if not isinstance(turns, list):
        return None

    identifiers = set()
    for item in turns:
        if not isinstance(item, dict):
            return None
        for name in ("attachments", "files"):
            resources = item.get(name) or []
            if not isinstance(resources, list):
                return None
            for resource in resources:
                if not isinstance(resource, dict):
                    return None
                identifier = resource.get("uuid") or resource.get("id")
                if not identifier:
                    return None
                identifiers.add(str(identifier))
    return len(identifiers)


def conversation_attachment_count(value):
    if not isinstance(value, dict) or "chat_messages" not in value:
        return None
    return _attachment_count(value["chat_messages"])


def _linearize_turns(turns: list[dict], leaf: str | None) -> list[dict]:
    if not any("parent_message_uuid" in turn for turn in turns):
        return turns
    by_id = {x.get("uuid"): x for x in turns}
    if leaf not in by_id:
        return turns
    branch, seen = [], set()
    while leaf in by_id and leaf not in seen:
        seen.add(leaf)
        item = by_id[leaf]
        branch.append(item)
        leaf = item.get("parent_message_uuid")
    return list(reversed(branch))


def _project_data(value: dict) -> ProjectData | None:
    raw_project = value.get("project") if isinstance(value.get("project"), dict) else {}
    pid = value.get("project_uuid") or raw_project.get("uuid")
    if not pid:
        return None
    pname = raw_project.get("name") or value.get("project_name") or ""
    return ProjectData(pid, str(pname))


def conversation(value, *, _action_uuid=None):
    if not isinstance(value, dict) or not value.get("uuid"):
        raise ClaudeCompatibilityError("Claude conversation response contract changed")
    messages = []
    raw_turns = value.get("chat_messages")
    turns = (
        _linearize_turns(raw_turns, value.get("current_leaf_message_uuid"))
        if isinstance(raw_turns, list)
        else []
    )
    if _action_uuid is not None:
        turns = [item for item in turns if item.get("uuid") == _action_uuid]
    pending_files = ()
    pending_sources = ()
    pending_id = ""

    def flush_files():
        if pending_files:
            messages.append(
                MessageData(
                    pending_id + ":files",
                    "assistant",
                    (Content("text", ""),),
                    "final",
                    {"files": pending_files},
                )
            )

    for item in turns:
        role = "user" if item.get("sender") == "human" else "assistant"
        if role == "user":
            flush_files()
            pending_files = ()
            pending_sources = ()
        content = item.get("content")
        native_blocks = bool(content)
        if native_blocks:
            last_text_index = next(
                (
                    i
                    for i in range(len(content) - 1, -1, -1)
                    if content[i].get("type") == "text" and content[i].get("text")
                ),
                None,
            )
        else:
            text = item.get("text")
            content = ({"type": "text", "text": text},) if text else ()
            last_text_index = 0 if content else None
        if role == "assistant":
            if native_blocks:
                search_results = [
                    result
                    for block in content
                    if block.get("type") == "web_search_tool_result"
                    or (block.get("type") == "tool_result" and block.get("name") == "web_search")
                    for result in (
                        block.get("content")[:128] if isinstance(block.get("content"), list) else ()
                    )
                    if isinstance(result, dict)
                ]
                pending_sources = sources_from(
                    {"sources": list(pending_sources)},
                    {"sources": search_results},
                    *(block for block in content if block.get("type") == "text"),
                )
            elif pending_sources:
                pending_sources = sources_from({"sources": pending_sources})
            pending_files = files_from(
                {"files": pending_files},
                item,
                *({"files": presented_files(block)} for block in content),
                rule=CLAUDE_FILE_RULE,
            )
            if pending_files:
                pending_id = str(item["uuid"])
        native_message = {k: v for k, v in item.items() if k != "content"}
        for index, block in enumerate(content):
            kind = block.get("type")
            if kind in ("text", "thinking"):
                text = (
                    block.get("text")
                    if kind == "text"
                    else block.get("thinking") or block.get("text")
                )
                if text:
                    is_last_text = kind == "text" and index == last_text_index
                    messages.append(
                        MessageData(
                            str(item["uuid"]) + ":" + str(index),
                            role,
                            (Content("text", text),),
                            "analysis" if kind == "thinking" else "final",
                            {
                                "native": block,
                                "native_message": native_message,
                                "sources": (
                                    pending_sources
                                    if role == "assistant" and is_last_text
                                    else sources_from(block)
                                    if native_blocks
                                    else ()
                                ),
                                "files": (
                                    pending_files if role == "assistant" and is_last_text else ()
                                ),
                            },
                            actions=actions_for(role) if is_last_text else (),
                        )
                    )
            else:
                # Keep unsupported tool/media blocks available to future consumers.
                messages.append(
                    MessageData(
                        str(item["uuid"]) + ":" + str(index),
                        role,
                        (Content(kind or "unknown", metadata={"native": block}),),
                        metadata={"native": block},
                    )
                )
        for resource_index, resource in enumerate(
            (item.get("attachments") or []) + (item.get("files") or [])
        ):
            messages.append(
                MessageData(
                    str(item["uuid"]) + ":resource:" + str(resource_index),
                    role,
                    (
                        Content(
                            "resource",
                            resource=Attachment(
                                str(resource.get("uuid") or resource.get("id") or ""),
                                resource.get("file_name") or resource.get("name") or "File",
                                metadata={"native": resource},
                            ),
                        ),
                    ),
                )
            )
        if last_text_index is not None and role == "assistant":
            pending_files = ()
            pending_sources = ()
    flush_files()
    return Conversation(
        value["uuid"],
        value.get("name") or "Claude Chat",
        tuple(messages),
        _project_data(value),
        {"native": {k: v for k, v in value.items() if k != "chat_messages"}},
    )


def stream_message_uuid(message_id: str) -> str | None:
    uuid, separator, block = message_id.rpartition(":")
    return uuid if uuid and separator and block.isascii() and block.isdecimal() else None


def action_target(history: dict, message_id: str, action: MessageAction) -> MessageData:
    native_id = stream_message_uuid(message_id)
    messages = conversation(history, _action_uuid=native_id).messages
    role = "user" if action == MessageAction.EDIT else "assistant"
    try:
        return target_message(messages, message_id, role)[1]
    except ProviderError:
        if any(item.id == message_id for item in messages):
            raise
        if native_id is not None:
            for item in reversed(messages):
                if (
                    item.role == role
                    and item.channel != "analysis"
                    and action.value in item.actions
                    and item.metadata.get("native_message", {}).get("uuid") == native_id
                ):
                    return item
        raise


def project(value):
    if not isinstance(value, dict) or not value.get("uuid"):
        raise ClaudeCompatibilityError("Claude project response contract changed")
    resources = tuple(
        Attachment(
            str(x.get("uuid") or x.get("id") or ""),
            x.get("file_name") or x.get("name") or "Resource",
            metadata={"native": x},
        )
        for x in (value.get("docs") or []) + (value.get("files") or [])
    )
    return ProjectData(
        value["uuid"],
        value.get("name") or "Claude projesi",
        value.get("prompt_template") or value.get("instructions"),
        resources,
        {"kind": "project", "native": value},
    )
