from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import Any

from ...base import BaseProvider, Capability, ProviderType
from ...errors import FeatureUnavailable
from ...resources.message.attachment import GROK_ATTACHMENTS
from ...resources.message.protocol import (
    ProviderRequest,
)
from ...resources.message.types import MessageAction
from ...resources.project.types import source_file


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


class BaseGrokProvider(BaseProvider):
    """Provider identity and protocol rules shared by both execution modes."""

    provider_type = ProviderType.GROK

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
