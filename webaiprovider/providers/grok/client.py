"""Origin-scoped blocking Grok web client and workspace editor."""

from __future__ import annotations

import mimetypes
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

from ...errors import AccountPaused, ProviderError
from ...resources.project.types import ProjectData
from ...transport import HttpSession
from ...transport.frames import SSEFrameDecoder, iter_bounded_lines
from .base import (
    _MAX_LINE_BYTES,
    _MAX_STREAM_BYTES,
    ORIGIN,
    BaseGrokClient,
    BaseGrokProjectEditor,
    _b64_ascii,
    remote_id,
)
from .records import conversation

__all__ = ["GrokClient", "GrokProjectEditor"]


class GrokClient(BaseGrokClient):
    """Blocking Grok HTTP client running directly on the caller thread."""

    def __init__(self, credential_file, *, client_factory=None, transport=None):
        self.session = HttpSession(ORIGIN, credential_file, None, client_factory=client_factory)
        self.http = transport or self.session
        self._validate_transport_origin(transport)
        self._auth_lock = threading.Lock()

    def authenticate(self):
        with self._auth_lock:
            return self._check_authentication()

    def _ensure_authenticated(self):
        if not self.session.authenticated:
            with self._auth_lock:
                if not self.session.authenticated:
                    self._check_authentication()

    def _check_authentication(self):
        try:
            status = self.http.request("GET", "/rest/user-settings")
            verified = self._validate_auth_status(status)
        except AccountPaused:
            self.session.authenticated = False
            raise
        self.session.authenticated = True
        return verified

    def request(self, method: str, path: str, **options: Any) -> Any:
        self._ensure_authenticated()
        try:
            return self.http.request(method, path, **options)
        except AccountPaused:
            self.session.authenticated = False
            raise

    def upload_file(self, path=None, *, content=None, name=None, mime_type=None) -> dict[str, Any]:
        filename, resolved_mime, payload_size, encoded = self._prepare_chat_upload(
            path, content=content, name=name, mime_type=mime_type
        )
        result = self.request(
            "POST",
            "/rest/app-chat/upload-file-v2/complete",
            json=self._chat_upload_body(filename, resolved_mime, encoded),
        )
        return self._chat_upload_result(result, filename, resolved_mime, payload_size)

    def events(self, path: str, body: dict[str, Any]) -> Iterator[dict[str, Any]]:
        self._ensure_authenticated()
        try:
            with self.http.stream(
                "POST",
                path,
                json=body,
                headers={"accept": "application/x-ndjson, text/event-stream"},
                allow_redirects=False,
                timeout=600,
            ) as response:
                decoder = SSEFrameDecoder(max_event_bytes=_MAX_LINE_BYTES, utf8_errors="strict")
                for line in iter_bounded_lines(
                    response.iter_bytes(),
                    max_line_bytes=_MAX_LINE_BYTES,
                    max_stream_bytes=_MAX_STREAM_BYTES,
                    error_label="Grok stream",
                ):
                    if line.startswith(("data:", "event:", "id:", "retry:", ":")) or not line:
                        for frame in decoder.feed_line(line):
                            if frame.data != "[DONE]":
                                yield self._parse_stream_json(frame.data)
                    elif line.strip() != "[DONE]":
                        yield self._parse_stream_json(line)
                for frame in decoder.feed(b"", final=True):
                    if frame.data != "[DONE]":
                        yield self._parse_stream_json(frame.data)
        except AccountPaused:
            self.session.authenticated = False
            raise

    def close(self) -> None:
        try:
            self.http.close()
        finally:
            if self.http is not self.session:
                self.session.close()
        self.session.authenticated = False


class GrokProjectEditor(BaseGrokProjectEditor):
    """Blocking Grok workspace mutations."""

    def __init__(self, transport: GrokClient):
        self.client = transport

    def options(self, project_id: str | None = None):
        return self._options(project_id)

    def update(self, project_id: str, **values: Any):
        body = self._update_body(values)
        result = self.client.request("PUT", "/rest/workspaces/" + remote_id(project_id), json=body)
        return self._finalize_update(project_id, body, result, values.get("current"))

    def read_metadata(self, project_id: str, current: ProjectData) -> ProjectData:
        data = self.client.request("GET", "/rest/workspaces/" + remote_id(project_id))
        return self._validate_metadata_response(project_id, current, data)

    def move_chat(
        self,
        conversation_id: str,
        project_id: str | None,
        *,
        source_project_id: str | None,
        fetch_metadata: Callable[[str], Any],
    ) -> None:
        if source_project_id == project_id:
            return
        conversation_path = "/conversations/" + remote_id(conversation_id)
        if project_id:
            self.client.request(
                "POST",
                "/rest/workspaces/" + remote_id(project_id) + "/conversations",
                json={"conversationId": conversation_id},
            )
        if source_project_id:
            try:
                self.client.request(
                    "DELETE",
                    "/rest/workspaces/" + remote_id(source_project_id) + conversation_path,
                )
            except Exception:
                if project_id:
                    try:
                        self.client.request(
                            "DELETE",
                            "/rest/workspaces/" + remote_id(project_id) + conversation_path,
                        )
                    except Exception:
                        raise ProviderError(
                            "The Grok conversation move did not complete; the conversation may remain in multiple workspaces."
                        ) from None
                raise
        for _ in range(5):
            updated = conversation(fetch_metadata(conversation_id))
            if (updated.project.id if updated.project is not None else None) == project_id:
                return
            time.sleep(0.1)
        raise ProviderError(
            "Grok conversation project membership could not be confirmed by the server"
        )

    def add_source(self, project_id: str, **values: Any):
        if values.get("kind") not in ("text", "file"):
            raise ValueError("Grok supports only text and file resources")
        if values["kind"] == "text":
            name, raw = self._prepare_text_source(values)
            mime_type, encoded = "text/plain", _b64_ascii(raw)
        else:
            name, mime_type, encoded = self._prepare_workspace_file(
                values["path"], guess_type=mimetypes.guess_type
            )
        name = self._validate_workspace_name(name, values.get("existing_resources", ()))
        result = self.client.request(
            "POST",
            "/rest/workspaces/" + remote_id(project_id) + "/files/upload",
            json={"fileName": name, "mimeType": mime_type, "content": encoded},
        )
        return self._source_result(result, name, mime_type)

    def remove_source(self, project_id: str, **values: Any):
        method, path, kwargs = self._resolve_remove_call(
            "/rest/workspaces/" + remote_id(project_id), values
        )
        return self.client.request(method, path, **kwargs)
