from __future__ import annotations

import math
from typing import Any

from ...base import BaseProvider, Capability, ProviderType
from ...resources.chat.types import Conversation
from ...resources.message.attachment import Attachment
from ...resources.message.types import Content, MessageData, actions_for
from ...resources.project.types import ProjectData
from .discovery import project_id
from .formats import message_files, message_parts


class BaseChatGPTProvider(BaseProvider):
    """Provider identity and protocol rules shared by both execution modes."""

    provider_type = ProviderType.CHATGPT

    _capabilities = frozenset(
        {
            Capability.CHAT,
            Capability.DELETE,
            Capability.PROJECT_DELETE,
            Capability.ATTACHMENTS,
            Capability.TEMPORARY,
            Capability.HISTORY,
            Capability.RENAME,
            Capability.AUTH,
            Capability.PROJECTS,
            Capability.PROJECT_CREATE,
            Capability.PROJECT_UPDATE,
            Capability.RESOURCES,
            Capability.PROJECT_MEMBERSHIP,
            Capability.MODELS,
            Capability.EFFORT,
            Capability.MESSAGE_EDIT,
            Capability.MESSAGE_RETRY,
            Capability.CHAT_BRANCH,
        }
    )

    def _project(self, raw: dict[str, Any]) -> ProjectData:
        return ProjectData(
            raw["id"],
            raw["name"],
            raw.get("instructions"),
            tuple(
                Attachment(
                    str(item.get("file_id") or item.get("id") or ""),
                    str(item.get("name") or ""),
                    item.get("type"),
                    metadata=dict(item),
                )
                for item in raw.get("resources", [])
            ),
            dict(raw.get("metadata") or {}),
        )

    def _message(self, item: dict[str, Any], *, allow_actions: bool) -> MessageData:
        content = item.get("content") or {}
        parts = message_parts(content)
        author = item.get("author")
        role = author.get("role", "unknown") if isinstance(author, dict) else "unknown"
        channel = item.get("channel")
        public_recipient = item.get("recipient") in (None, "all")
        summary = isinstance(content, dict) and content.get("content_type") in (
            "thoughts",
            "reasoning_recap",
            "reasoning_summary",
        )
        if summary or (role == "assistant" and not public_recipient):
            channel = "analysis"
        if len(parts) == 1 and isinstance(parts[0], str):
            blocks = (Content("text", text=parts[0]),)
        else:
            blocks = tuple(
                Content("text", text=part)
                if isinstance(part, str)
                else Content("text", text=part["text"], metadata={"native": part})
                if isinstance(part, dict) and isinstance(part.get("text"), str)
                else Content("provider", metadata={"value": part})
                for part in parts
            )
        message_id = item.get("id", "")
        return MessageData(
            str(message_id),
            role,
            blocks,
            channel,
            dict(item),
            actions=actions_for(role, branch=True)
            if allow_actions and message_id and channel != "analysis" and public_recipient
            else (),
        )

    @staticmethod
    def _active_messages(raw: dict[str, Any]) -> list[dict[str, Any]]:
        mapping = raw.get("mapping")
        if not isinstance(mapping, dict) or not mapping:
            messages = raw.get("messages")
            return messages if isinstance(messages, list) else []
        node_id = raw.get("current_node")
        if not isinstance(node_id, str) or node_id not in mapping:
            raise ValueError("ChatGPT conversation has no active message path")
        reversed_messages: list[dict[str, Any]] = []
        visited: set[str] = set()
        while node_id is not None:
            if node_id in visited:
                raise ValueError("ChatGPT conversation message path contains a cycle")
            visited.add(node_id)
            node = mapping.get(node_id)
            if not isinstance(node, dict):
                raise ValueError("ChatGPT conversation message path is incomplete")
            message = node.get("message")
            if isinstance(message, dict):
                reversed_messages.append(message)
            parent_id = node.get("parent")
            if parent_id is None:
                break
            if not isinstance(parent_id, str) or parent_id not in mapping:
                raise ValueError("ChatGPT conversation message path is incomplete")
            node_id = parent_id
        reversed_messages.reverse()
        return reversed_messages

    def _conversation(self, raw: dict[str, Any]) -> Conversation:
        remote_id = raw.get("conversation_id") or raw.get("id")
        if not isinstance(remote_id, str) or not remote_id:
            raise ValueError("provider conversation has no remote ID")
        allow_actions = not (
            raw.get("pending_branch") or raw.get("is_read_only") or raw.get("is_temporary_chat")
        )
        messages, turn = [], []
        for item in self._active_messages(raw):
            if not isinstance(item, dict):
                continue
            message = self._message(item, allow_actions=allow_actions)
            if message.role == "user":
                turn.clear()
            else:
                turn.append(item)
            if (
                message.role == "assistant"
                and message.channel != "analysis"
                and item.get("recipient") in (None, "all")
            ):
                message.metadata["files"] = message_files(*turn)
                turn.clear()
            messages.append(message)
        metadata = {key: value for key, value in raw.items() if key != "mapping"}
        latest_assistant = next(
            (message for message in reversed(messages) if message.role == "assistant"), None
        )
        if latest_assistant and latest_assistant.metadata.get("status") == "in_progress":
            generation: dict[str, Any] = {"active": True}
            started_at = latest_assistant.metadata.get("create_time")
            if (
                isinstance(started_at, (int, float))
                and not isinstance(started_at, bool)
                and 0 < started_at < 4_102_444_800
                and math.isfinite(started_at)
            ):
                generation["started_at"] = started_at
            metadata["generation"] = generation
        pid = project_id(raw)
        return Conversation(
            remote_id,
            raw.get("title") or "Untitled",
            tuple(messages),
            project=ProjectData(pid, "") if pid else None,
            metadata=metadata,
        )
