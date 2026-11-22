from __future__ import annotations

import base64
import mimetypes
import os
import stat
from dataclasses import replace
from pathlib import Path
from typing import Any
from urllib.parse import quote

from ...accounts import session_exists
from ...accounts.status import SessionState, SessionStatus
from ...base import BaseProvider, Capability, ProviderType
from ...collections import Page
from ...core.json import json_loads
from ...errors import AccountPaused, FeatureUnavailable, ProviderError, RateLimited
from ...resources.chat.types import Conversation
from ...resources.message.attachment import GROK_ATTACHMENTS, Attachment
from ...resources.message.protocol import ProviderRequest
from ...resources.message.types import MessageAction
from ...resources.model.collections import MODEL_CATALOG_TTL
from ...resources.project.types import ProjectData, ProjectOptions, source_file
from .records import active_responses, conversation, project

ORIGIN = "https://grok.com"
_MAX_LINE_BYTES = 1_048_576
_MAX_STREAM_BYTES = 64 * 1024 * 1024
_MAX_CHAT_FILE_BYTES = 3 * 1024 * 1024
_MAX_WORKSPACE_FILE_BYTES = 20 * 1024 * 1024
_CATALOG_TTL = MODEL_CATALOG_TTL
_CONTINUATION_TTL = 1800.0


def remote_id(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > 512:
        raise ValueError("Grok record ID is invalid")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("Grok record ID is invalid")
    return quote(value, safe="")


def _read_limited(
    path: Path,
    *,
    max_bytes: int = _MAX_WORKSPACE_FILE_BYTES,
    error: str = "Grok file is empty or exceeds the size limit",
) -> bytes:
    with path.open("rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= max_bytes:
            raise ValueError(error)
        data = stream.read(info.st_size + 1)
        if len(data) > info.st_size:
            data += stream.read(max_bytes + 1 - len(data))
    if not 0 < len(data) <= max_bytes:
        raise ValueError(error)
    return data


def _b64_ascii(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def validate_upload_candidate(path=None, *, content=None, name=None, mime_type=None):
    if content is not None:
        filename = name or (Path(path).name if path is not None else "attachment")
        if Path(filename).suffix.lower() not in GROK_ATTACHMENTS.allowed_extensions:
            raise ValueError("Grok chat file type is not supported")
        size = len(content)
        if not 0 < size <= GROK_ATTACHMENTS.max_bytes:
            raise ValueError("Grok chat file is empty or exceeds the 3 MiB limit")
        file = None
    elif path is not None:
        file = source_file(path, error="Grok chat files must not exceed 3 MiB")
        if file.suffix.lower() not in GROK_ATTACHMENTS.allowed_extensions:
            raise ValueError("Grok chat file type is not supported")
        size = file.stat().st_size
        if not 0 < size <= GROK_ATTACHMENTS.max_bytes:
            raise ValueError("Grok chat file is empty or exceeds the 3 MiB limit")
        filename = name or file.name
    else:
        raise ValueError("Grok chat file upload requires a file path or byte content")
    resolved_mime = mime_type or mimetypes.guess_type(filename)[0] or "application/octet-stream"
    return filename, resolved_mime, size, file


def _validated_grok_attachment(attachment: Any) -> tuple[dict[str, Any], int, str]:
    metadata = attachment.get("grok_file") if isinstance(attachment, dict) else None
    asset_id = (
        (metadata.get("assetId") or metadata.get("fileMetadataId"))
        if isinstance(metadata, dict)
        else None
    )
    size = attachment.get("size_bytes") if isinstance(attachment, dict) else None
    if (
        not isinstance(asset_id, str)
        or not asset_id
        or asset_id != attachment.get("id")
        or not isinstance(size, int)
        or isinstance(size, bool)
        or not 0 < size <= GROK_ATTACHMENTS.max_bytes
    ):
        raise ValueError("Grok chat attachment metadata is invalid")
    mime = str(
        attachment.get("mime_type")
        or (
            (metadata.get("mimeType") or metadata.get("fileMimeType"))
            if isinstance(metadata, dict)
            else ""
        )
        or ""
    ).lower()
    return metadata, size, mime


def _validate_grok_request(request: ProviderRequest) -> tuple[list[dict], list[tuple[int, dict]]]:
    if request.effort is not None:
        raise FeatureUnavailable(Capability.EFFORT, "Grok does not expose shared effort levels")
    if len(request.attachments) > GROK_ATTACHMENTS.max_files:
        raise ValueError("Too many Grok chat attachments")
    if request.temporary and request.project_id:
        raise FeatureUnavailable(Capability.TEMPORARY, "Temporary chats cannot belong to a project")
    if request.action not in (None, MessageAction.RETRY, MessageAction.EDIT):
        raise FeatureUnavailable(request.action.capability)
    if request.action == MessageAction.RETRY and not request.message_id:
        raise ValueError("Grok retry requires the selected assistant response")
    if request.action == MessageAction.EDIT and not request.message_id:
        raise ValueError("Grok edit requires the selected user prompt")

    total_attachment_bytes = 0
    pending_uploads: list[tuple[int, dict]] = []
    prepared: list[dict] = list(request.attachments)
    for index, attachment in enumerate(request.attachments):
        if (
            isinstance(attachment, dict)
            and attachment.get("grok_file") is None
            and ("bytes" in attachment or "path" in attachment)
        ):
            _, _, size, _ = validate_upload_candidate(
                attachment.get("path"),
                content=attachment.get("bytes"),
                name=attachment.get("name"),
                mime_type=attachment.get("mime_type"),
            )
            pending_uploads.append((index, attachment))
        else:
            _, size, _ = _validated_grok_attachment(attachment)
        total_attachment_bytes += size
    if (
        GROK_ATTACHMENTS.max_total_bytes
        and total_attachment_bytes > GROK_ATTACHMENTS.max_total_bytes
    ):
        raise ValueError("Grok chat attachments exceed the total size limit")
    return prepared, pending_uploads


def _inherit_grok_edit_attachments(
    target: dict, file_attachments: list, image_attachments: list
) -> None:
    if file_attachments or image_attachments:
        return
    seen_assets: set[str] = set()
    entries = [
        *((False, entry) for entry in (target.get("fileAttachments") or ())),
        *((True, entry) for entry in (target.get("imageAttachments") or ())),
    ]
    for is_image, item in entries:
        if isinstance(item, str) and item:
            if item in seen_assets:
                continue
            seen_assets.add(item)
            (image_attachments if is_image else file_attachments).append(item)
        elif isinstance(item, dict):
            aid = item.get("assetId") or item.get("fileMetadataId") or item.get("id")
            if aid and aid in seen_assets:
                continue
            if aid:
                seen_assets.add(aid)
            mime = str(item.get("mimeType") or item.get("fileMimeType") or "").lower()
            serialized = _grok_attachment_entry(item)
            if mime.startswith("image/") or is_image:
                image_attachments.append(serialized)
            else:
                file_attachments.append(serialized)


def _grok_attachment_entry(metadata: dict) -> dict | str:
    """Serialize uploaded Grok metadata for conversation creation/edit payloads."""
    if "fileMetadataId" in metadata and "assetId" not in metadata:
        return metadata["fileMetadataId"]
    return metadata


class BaseGrokClient:
    """Shared non-I/O protocol helpers for GrokClient and AsyncGrokClient."""

    ORIGIN = ORIGIN
    session: Any
    http: Any

    def _validate_transport_origin(self, transport: Any) -> None:
        if transport is not None and self.http.origin != self.ORIGIN:
            raise ValueError("Provider transport origin does not match its account")

    @staticmethod
    def _validate_auth_status(status: Any) -> dict[str, Any]:
        if not isinstance(status, dict):
            raise AccountPaused(
                "The Grok session could not be verified; verify it again in the Accounts section."
            )
        return status

    @staticmethod
    def _prepare_chat_upload(path=None, *, content=None, name=None, mime_type=None):
        filename, resolved_mime, _, file = validate_upload_candidate(
            path, content=content, name=name, mime_type=mime_type
        )
        payload = (
            content
            if content is not None
            else _read_limited(
                file,
                max_bytes=_MAX_CHAT_FILE_BYTES,
                error="Grok chat file is empty or exceeds the 3 MiB limit",
            )
        )
        return filename, resolved_mime, len(payload), _b64_ascii(payload)

    @staticmethod
    def _chat_upload_body(filename: str, resolved_mime: str, encoded: str) -> dict[str, Any]:
        return {
            "directUpload": {
                "fileData": encoded,
                "fileName": filename,
                "fileMimeType": resolved_mime,
                "fileSource": "SELF_UPLOAD_FILE_SOURCE",
            }
        }

    @staticmethod
    def _chat_upload_result(
        result: Any, filename: str, resolved_mime: str, payload_size: int
    ) -> dict[str, Any]:
        metadata = result.get("fileMetadata") if isinstance(result, dict) else None
        asset_id = (
            (metadata.get("assetId") or metadata.get("fileMetadataId"))
            if isinstance(metadata, dict)
            else None
        )
        if (
            not isinstance(result, dict)
            or not isinstance(metadata, dict)
            or result.get("terminalError")
            or not isinstance(asset_id, str)
            or not asset_id
        ):
            raise ProviderError("Grok chat file upload failed")
        return {
            "id": asset_id,
            "name": metadata.get("name") or metadata.get("fileName") or filename,
            "mime_type": metadata.get("mimeType") or metadata.get("fileMimeType") or resolved_mime,
            "size_bytes": payload_size,
            "grok_file": metadata,
        }

    @staticmethod
    def _parse_stream_json(raw: str) -> dict[str, Any]:
        try:
            event = json_loads(raw)
        except ValueError:
            raise ProviderError("Grok stream response format has changed") from None
        if not isinstance(event, dict):
            raise ProviderError("Grok stream response format has changed")
        return event


class BaseGrokProjectEditor:
    """Shared non-I/O workspace validation and serialization for Grok project editors."""

    @staticmethod
    def _options(project_id: str | None = None) -> ProjectOptions:
        return ProjectOptions(
            fields=("name", "instructions"),
            sources=(
                {"id": "text", "title": "Text"},
                {"id": "file", "title": "File", "accept": "*/*"},
            )
            if project_id
            else (),
            remove_sources=bool(project_id),
        )

    @staticmethod
    def _update_body(values: dict[str, Any]) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if "name" in values:
            name = values["name"]
            if not isinstance(name, str) or not 1 <= len(name.strip()) <= 200:
                raise ValueError("Project name must be between 1 and 200 characters")
            body["name"] = name.strip()
        if "instructions" in values:
            if not isinstance(values["instructions"], str):
                raise ValueError("Project instructions must be text")
            body["customPersonality"] = values["instructions"]
        if not body:
            raise ValueError("A Grok project change is required")
        return body

    @staticmethod
    def _apply_metadata(current: ProjectData, data: dict[str, Any]) -> ProjectData:
        metadata = dict(current.metadata)
        for key, native in (("preferred_model", "preferredModel"), ("kind", "kind")):
            if native in data:
                metadata[key] = data[native]
        return replace(
            current,
            name=data.get("name", current.name),
            instructions=data.get("customPersonality", current.instructions),
            metadata=metadata,
        )

    @staticmethod
    def _prepare_workspace_file(path: Any, *, guess_type=None) -> tuple[str, str, str]:
        file = source_file(path, error="Grok workspace files must not exceed 20 MiB")
        raw_bytes = _read_limited(
            file,
            max_bytes=_MAX_WORKSPACE_FILE_BYTES,
            error="Grok workspace files must not exceed 20 MiB",
        )
        guesser = guess_type or mimetypes.guess_type
        resolved_mime = (
            "text/markdown"
            if file.suffix.casefold() in {".md", ".markdown"}
            else guesser(file.name)[0] or "text/plain"
        )
        return file.name, resolved_mime, _b64_ascii(raw_bytes)

    @staticmethod
    def _validate_workspace_name(name: str, existing_resources: Any) -> str:
        cleaned = name.strip()
        if not cleaned or len(cleaned) > 255 or any(char in cleaned for char in "/\\\r\n\x00"):
            raise ValueError("Grok resource name is invalid")
        if any(
            (source.metadata.get("path") or source.name).rsplit("/", 1)[-1] == cleaned
            for source in existing_resources
        ):
            raise ValueError("A Grok project resource with this name already exists")
        return cleaned

    @staticmethod
    def _source_result(result: Any, name: str, mime_type: str) -> Any:
        item = (
            result["file"]
            if isinstance(result, dict) and isinstance(result.get("file"), dict)
            else result
        )
        if isinstance(item, dict) and (item.get("path") or item.get("assetId")):
            return Attachment(
                str(item.get("path") or item.get("assetId")),
                str(item.get("name") or name),
                item.get("mimeType") or mime_type,
                metadata={
                    key: item[key]
                    for key in ("path", "assetId", "size", "createdAt", "modifiedAt")
                    if key in item
                },
            )
        return result

    @classmethod
    def _finalize_update(
        cls, project_id: str, body: dict[str, Any], result: Any, current: Any
    ) -> Any:
        if (
            isinstance(current, ProjectData)
            and isinstance(result, dict)
            and (result.get("workspaceId") or result.get("id") or project_id) == project_id
            and all(result.get(key) == value for key, value in body.items())
        ):
            return cls._apply_metadata(current, result)
        return result

    @classmethod
    def _validate_metadata_response(
        cls, project_id: str, current: ProjectData, data: Any
    ) -> ProjectData:
        if not isinstance(data, dict) or (data.get("workspaceId") or data.get("id")) != project_id:
            raise ProviderError("Grok workspace response contract changed")
        return cls._apply_metadata(current, data)

    @staticmethod
    def _prepare_text_source(values: dict[str, Any]) -> tuple[str, bytes]:
        name, content = values.get("name"), values.get("text")
        if not isinstance(name, str) or not isinstance(content, str) or not content:
            raise ValueError("Resource name and text content are required")
        raw = content.encode("utf-8")
        if not 0 < len(raw) <= _MAX_WORKSPACE_FILE_BYTES:
            raise ValueError("Grok workspace files must not exceed 20 MiB")
        return name, raw

    @staticmethod
    def _resolve_remove_call(
        project_path: str, values: dict[str, Any]
    ) -> tuple[str, str, dict[str, Any]]:
        source = values["resource"]
        if source.id != values["resource_id"]:
            raise ValueError("Grok project resource was not found")
        item = source.metadata
        if item.get("path"):
            return (
                "DELETE",
                project_path + "/files",
                {"params": {"path": item["path"], "recursive": "false"}},
            )
        if item.get("assetId"):
            return "DELETE", project_path + "/assets/" + remote_id(item["assetId"]), {}
        raise ValueError("Grok project resource was not found")


class BaseGrokProvider(BaseProvider):
    """Provider identity and protocol rules shared by both execution modes."""

    provider_type = ProviderType.GROK
    client: Any
    _continuations: Any

    _capabilities = frozenset(
        {
            Capability.CHAT,
            Capability.TEMPORARY,
            Capability.AUTH,
            Capability.ATTACHMENTS,
            Capability.HISTORY,
            Capability.RENAME,
            Capability.DELETE,
            Capability.MODELS,
            Capability.PROJECTS,
            Capability.PROJECT_CREATE,
            Capability.PROJECT_UPDATE,
            Capability.PROJECT_DELETE,
            Capability.PROJECT_MEMBERSHIP,
            Capability.RESOURCES,
            Capability.MESSAGE_RETRY,
            Capability.MESSAGE_EDIT,
        }
    )

    def _send_continuation(self, request, conversation_id: str) -> tuple[str | None, bool]:
        needs_history = request.action in (MessageAction.RETRY, MessageAction.EDIT)
        parent = self._take_continuation(conversation_id)
        return (None if needs_history else parent), needs_history

    def _take_continuation(self, conversation_id: str) -> str | None:
        return self._continuations.remove(conversation_id)

    def _remember_continuation(self, conversation_id: str, response_id: str) -> None:
        if (
            not isinstance(conversation_id, str)
            or not isinstance(response_id, str)
            or not response_id
        ):
            return
        self._continuations.put(conversation_id, response_id)

    def _parse_chats_page(self, data: Any) -> Page[Conversation]:
        if not isinstance(data, dict):
            raise ProviderError("Grok conversation list response contract changed")
        conversations = data.get("conversations")
        if conversations is not None and not isinstance(conversations, list):
            raise ProviderError("Grok conversation list response contract changed")
        return Page(
            tuple(conversation(item) for item in (conversations or ())),
            data.get("nextPageToken"),
        )

    def _build_chat_from_history(
        self, conversation_id: str, metadata: Any, history: Any
    ) -> Conversation:
        active = active_responses(history)
        chat = conversation(metadata, active_rows=active)
        inflight = history.get("inflightResponses") if isinstance(history, dict) else None
        if isinstance(inflight, list) and inflight:
            return Conversation(
                chat.id,
                chat.title,
                chat.messages,
                chat.project,
                {**chat.metadata, "generation": {"active": True}},
            )
        if active:
            self._remember_continuation(conversation_id, active[-1]["responseId"])
        return chat

    def _parse_projects_page(self, data: Any) -> Page[ProjectData]:
        if not isinstance(data, dict):
            raise ProviderError("Grok workspace list response contract changed")
        workspaces = data.get("workspaces")
        if workspaces is not None and not isinstance(workspaces, list):
            raise ProviderError("Grok workspace list response contract changed")
        return Page(
            tuple(project(item) for item in (workspaces or ())),
            data.get("nextPageToken"),
        )

    def _parse_project_detail(self, workspace: Any, listing: Any) -> ProjectData:
        if not isinstance(listing, dict):
            raise ProviderError("Grok workspace file response contract changed")
        files = listing.get("files")
        if files is not None and not isinstance(files, list):
            raise ProviderError("Grok workspace file response contract changed")
        return project(workspace, files)

    def _parse_project_chats_page(self, project_id: str, data: Any) -> Page[Conversation]:
        if not isinstance(data, dict):
            raise ProviderError("Grok project conversation response contract changed")
        conversations = data.get("conversations")
        if conversations is not None and not isinstance(conversations, list):
            raise ProviderError("Grok project conversation response contract changed")
        return Page(
            tuple(
                conversation({**item, "workspaceId": item.get("workspaceId") or project_id})
                for item in (conversations or ())
            ),
            data.get("nextPageToken"),
        )

    @staticmethod
    def _split_attachments(prepared: list[dict]) -> tuple[list, list]:
        file_attachments = []
        image_attachments = []
        for attachment in prepared:
            metadata, _, mime = _validated_grok_attachment(attachment)
            entry = _grok_attachment_entry(metadata)
            (image_attachments if mime.startswith("image/") else file_attachments).append(entry)
        return file_attachments, image_attachments

    @staticmethod
    def _resolve_parent_from_history(
        request: ProviderRequest,
        history: Any,
        file_attachments: list,
        image_attachments: list,
    ) -> str:
        is_retry = request.action == MessageAction.RETRY
        is_edit = request.action == MessageAction.EDIT
        if not isinstance(history, dict):
            raise ProviderError("Grok conversation history response contract changed")
        if not is_retry and not is_edit and not isinstance(history.get("responseNodes"), list):
            raise ProviderError("Grok conversation node response contract changed")
        if history.get("inflightResponses"):
            raise ProviderError("Grok conversation already has a response in progress")
        active = active_responses(history)
        if not (is_retry or is_edit):
            return (active[-1].get("responseId") or "") if active else ""
        target = next(
            (item for item in active if item.get("responseId") == request.message_id),
            None,
        )
        expected_senders = ("assistant", "model", "grok") if is_retry else ("human", "user")
        if target is None or str(target.get("sender", "")).lower() not in expected_senders:
            label = "retry" if is_retry else "edit"
            raise ProviderError(f"Grok {label} target is no longer on the active conversation path")
        if is_edit:
            _inherit_grok_edit_attachments(target, file_attachments, image_attachments)
        return target.get("parentResponseId") or ""

    @staticmethod
    def _build_stream_payload(
        request: ProviderRequest,
        *,
        selected_id: str,
        model: str | None,
        model_mode: str | None,
        parent_response_id: str,
        file_attachments: list,
        image_attachments: list,
    ) -> tuple[str, dict[str, object]]:
        is_retry = request.action == MessageAction.RETRY
        body: dict[str, object] = {
            "message": "" if is_retry else request.prompt,
            "parentResponseId": parent_response_id,
            "fileAttachments": file_attachments,
            "imageAttachments": image_attachments,
            "disableSearch": False,
            "temporary": request.temporary,
            "isRegenRequest": is_retry,
        }
        if model_mode:
            body["modelMode"] = model_mode
        if model or not model_mode:
            body["modelName"] = model or selected_id
        if request.project_id:
            body["workspaceIds"] = [request.project_id]
        path = (
            "/rest/app-chat/conversations/new"
            if not request.conversation_id
            else "/rest/app-chat/conversations/" + remote_id(request.conversation_id) + "/responses"
        )
        return path, body

    def _raise_stream_error(self, error: object) -> None:
        code = str(error.get("code") or "").upper() if isinstance(error, dict) else ""
        msg = str(error.get("message") or "") if isinstance(error, dict) else str(error)
        if code in ("UNAUTHORIZED", "AUTH_REQUIRED"):
            self.client.session.authenticated = False
            raise AccountPaused("Grok session expired; verify the account again")
        if (
            "RATE_LIMIT" in code
            or "QUOTA" in code
            or "TOO_MANY_REQUESTS" in code
            or "rate limit" in msg.lower()
            or "too many requests" in msg.lower()
        ):
            retry_after = error.get("retryAfter") if isinstance(error, dict) else None
            try:
                delay = float(retry_after) if retry_after is not None else None
            except (ValueError, TypeError):
                delay = None
            raise RateLimited(delay)
        raise ProviderError("Grok stream reported an error; the request was not resent")

    def _active_transport(self) -> Any:
        return self.client.http

    def _session_status(self) -> SessionStatus:
        saved = session_exists(self.credential_file)
        if self.client.session.authenticated:
            state = SessionState.AUTHENTICATED
        elif saved:
            state = SessionState.UNKNOWN
        else:
            state = SessionState.LOGIN_REQUIRED
        return SessionStatus(state, saved)

    @staticmethod
    def _build_chats_query(cursor: str | None, limit: int) -> dict[str, int | str]:
        query: dict[str, int | str] = {"pageSize": max(1, min(int(limit), 100))}
        if cursor:
            query["pageToken"] = cursor
        return query

    @staticmethod
    def _build_projects_query(cursor: str | None) -> dict[str, int | str]:
        query: dict[str, int | str] = {"pageSize": 50}
        if cursor:
            query["pageToken"] = cursor
        return query

    @staticmethod
    def _build_project_chats_query(
        project_id: str, cursor: str | None, limit: int | None
    ) -> dict[str, int | str]:
        query: dict[str, int | str] = {"pageSize": min(limit or 50, 50), "workspaceId": project_id}
        if cursor:
            query["pageToken"] = cursor
        return query

    @staticmethod
    def _validate_chat_title(title: Any) -> str:
        if not isinstance(title, str) or not 1 <= len(title.strip()) <= 200:
            raise ValueError("Conversation title must be between 1 and 200 characters")
        return title.strip()

    @staticmethod
    def _validate_create_project(name: Any, instructions: Any) -> dict[str, str]:
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 200:
            raise ValueError("Project name must be between 1 and 200 characters")
        if not isinstance(instructions, str):
            raise ValueError("Project instructions must be text")
        return {"name": name.strip(), "customPersonality": instructions or ""}

    @staticmethod
    def _extract_model_spec(
        catalog: Any, requested_model: str | None
    ) -> tuple[str, str | None, str | None]:
        selected = (
            catalog.chat_model(requested_model)
            if requested_model
            else catalog.chat_model(catalog.default_model)
        )
        if selected is None:
            raise ProviderError("Selected Grok model is unavailable for this account")
        model = selected.metadata.get("modelName")
        model_mode = selected.metadata.get("modelMode")
        if not isinstance(model_mode, str) or not model_mode:
            model_mode = None
        return selected.id, model, model_mode
