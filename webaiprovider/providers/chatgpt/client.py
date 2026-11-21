"""Native blocking ChatGPT web client and project editor (zero asyncio)."""

from __future__ import annotations

import os
import stat
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin

from curl_cffi.requests import Session as CurlSession
from curl_cffi.requests.exceptions import RequestException as CurlRequestException

from ...core.file_stat import same_file_version
from ...core.timing import _STREAM_PAUSED, SendTiming, current_send_timing
from ...errors import RemoteNotFound
from ...resources.message.protocol import ProviderRequest
from ...resources.message.types import MessageAction
from ...transport import HTTPTransport, Transport, TransportConfig
from ...transport.frames import SSEFrameDecoder
from ...transport.http import _message_stream, _wait_for_stream_head
from .auth import BrowserSession
from .base import BaseChatGPTClient, BaseChatGPTProjectEditor, _ChatGPTSSEProcessor
from .catalogs import parse_model_catalog
from .constants import CHATGPT_ORIGIN, DEFAULT_PAGE_SIZE
from .errors import ChatGPTAuthenticationError, ChatGPTError
from .sentinel import SentinelRuntime

__all__ = [
    "ChatGPTClient",
    "ChatGPTProjectEditor",
    "prepare_chatgpt_attachments",
]


def prepare_chatgpt_attachments(client: Any, request: ProviderRequest) -> list[Any]:
    attachments: list[Any] = []
    for item in request.attachments:
        if isinstance(item, dict) and ("bytes" in item or "path" in item):
            attachments.append(
                client.upload_file(
                    item.get("name")
                    or (Path(item["path"]).name if item.get("path") else "attachment"),
                    item.get("mime_type") or "application/octet-stream",
                    item["bytes"] if item.get("bytes") is not None else Path(item["path"]),
                    project_id=request.project_id or None,
                )
            )
        else:
            attachments.append(item)
    return attachments


class ChatGPTClient(BaseChatGPTClient):
    """Native blocking ChatGPT client running directly on the caller thread."""

    def __init__(
        self,
        session: BrowserSession,
        *,
        authenticate: bool = True,
        transport: Transport | None = None,
    ) -> None:
        self._init_client_state(session, authenticate=authenticate)
        self._sentinel_lock = threading.Lock()
        self._refresh_lock = threading.Lock()
        self._catalog_lock = threading.Lock()
        self._owned_transport = transport is None
        timing = current_send_timing()
        started = time.perf_counter_ns() if timing is not None else None
        if transport is None:
            from ...transport.network import create_curl_share

            self.transport: Transport = HTTPTransport(
                CHATGPT_ORIGIN,
                client_factory=lambda timeout=(10.0, 30.0): CurlSession(
                    impersonate="chrome",
                    trust_env=False,
                    timeout=timeout,
                    discard_cookies=True,
                    curl_share=create_curl_share(),
                ),
                config=TransportConfig(timeout=(10.0, 30.0)),
                headers=self._transport_headers,
            )
            if started is not None:
                timing.record("http_client_construct_ms", time.perf_counter_ns() - started)
        else:
            self.transport = transport

    def __enter__(self) -> ChatGPTClient:
        self.ensure_auth()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        self._clear_local_state()
        raw_streams = getattr(self, "_active_streams", None)
        active = (
            list(raw_streams.values()) if isinstance(raw_streams, dict) else list(raw_streams or ())
        )
        for stream in active:
            close = getattr(stream, "close", None)
            if callable(close):
                with suppress(Exception):
                    close()
        upload_http = getattr(self, "_upload_http", None)
        self._upload_http = None
        if upload_http is not None:
            with suppress(Exception):
                upload_http.close()
        if getattr(self, "_owned_transport", False):
            transport = getattr(self, "transport", None)
            if transport is not None:
                with suppress(Exception):
                    transport.close()

    @contextmanager
    def _tracked_stream(self, method: str, path: str, *, _message=False, **options):
        if (
            options.get("inherit_headers", True)
            and "headers" not in options
            and getattr(self, "headers", None) is not None
        ):
            options["headers"] = self._transport_headers(method, path)
        opening = (
            _message_stream(self.transport, method, path, **options)
            if _message
            else self.transport.stream(method, path, **options)
        )
        with opening as stream:
            stream_key = id(stream)
            self._active_streams[stream_key] = stream
            try:
                if self._is_closed():
                    close = getattr(stream, "close", None)
                    if callable(close):
                        with suppress(Exception):
                            close()
                yield stream
            finally:
                self._active_streams.pop(stream_key, None)

    def refresh_auth(self) -> None:
        try:
            self.list_url, self.headers = self.session.load()
            self._authenticated = True
            self._needs_initial_auth = False
        except ChatGPTError:
            raise ChatGPTError(
                "No saved session; verify the account in the Accounts section"
            ) from None

    def ensure_auth(self) -> None:
        if not self._needs_initial_auth or self._authenticated:
            return
        with self._refresh_lock:
            if self._needs_initial_auth and not self._authenticated:
                timing = current_send_timing()
                started = time.perf_counter_ns() if timing is not None else None
                self.refresh_auth()
                if started is not None:
                    timing.record("provider_auth_refresh_ms", time.perf_counter_ns() - started)

    def refresh_session(self) -> None:
        headers = self.headers.copy()
        headers.pop("authorization", None)
        headers["accept-encoding"] = "gzip, deflate"
        response = self.transport.send(
            "GET",
            "/api/auth/session",
            check_status=False,
            inherit_headers=False,
            headers=headers,
            timeout=30,
        )
        self.session.save(self.list_url, self._apply_refreshed_session(response))

    def request(self, method: str, url: str, *, expect_json: bool = True, **kwargs: Any) -> Any:
        target_path, full_path = self._validate_request_url(url)
        self.ensure_auth()
        for attempt in range(2):
            request_headers = self._transport_headers(method, target_path)
            response = self.transport.send(
                method, full_path, headers=request_headers, check_status=False, **kwargs
            )
            if (
                response.status_code not in (401, 403)
                or attempt == 1
                or method.upper() not in ("GET", "HEAD")
            ):
                break
            failed_auth = request_headers.get("authorization")
            with self._refresh_lock:
                current_auth = self.headers.get("authorization")
                if not current_auth or current_auth == failed_auth:
                    try:
                        self.refresh_session()
                    except ChatGPTError:
                        raise ChatGPTAuthenticationError(
                            "The session could not be refreshed; verify it again in the Accounts section."
                        ) from None
        return self._check_api_response(method, target_path, response, expect_json=expect_json)

    def verify(self) -> dict[str, Any]:
        return self.list_page(0, DEFAULT_PAGE_SIZE)

    def model_catalog(self, *, refresh: bool = False):
        cached = getattr(self, "_model_catalog", None)
        if cached is not None and not refresh and time.monotonic() - cached[0] < 300:
            return cached[1]
        with self._catalog_lock:
            cached = getattr(self, "_model_catalog", None)
            if cached is not None and not refresh and time.monotonic() - cached[0] < 300:
                return cached[1]
            raw = self.request(
                "GET",
                urljoin(CHATGPT_ORIGIN, "/backend-api/models"),
                params={
                    "iim": "false",
                    "is_gizmo": "false",
                    "supports_model_picker_upgrade_presets": "true",
                },
            ).json()
            catalog = parse_model_catalog(raw)
            self._model_catalog = (time.monotonic(), catalog)
            return catalog

    def list_projects(self, cursor: str | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {"conversations_per_gizmo": 0, "limit": 20, "owned_only": "false"}
        if cursor is not None:
            params["cursor"] = cursor
        raw = self.request(
            "GET", urljoin(CHATGPT_ORIGIN, "/backend-api/gizmos/snorlax/sidebar"), params=params
        ).json()
        return self._parse_projects_page(raw)

    def create_project(self, name, instructions="", *, memory_share=True):
        cleaned, memory_scope, payload = self._prepare_create_project(
            name, instructions, memory_share
        )
        raw = self.request(
            "POST", urljoin(CHATGPT_ORIGIN, "/backend-api/projects"), json=payload
        ).json()
        project, needs_refetch = self._parse_created_project(
            raw, cleaned, instructions, memory_scope
        )
        return self.get_project(project["id"]) if needs_refetch else project

    def get_project(self, project_id: str) -> dict[str, Any]:
        raw = self.request(
            "GET", urljoin(CHATGPT_ORIGIN, f"/backend-api/gizmos/{quote(project_id, safe='')}")
        ).json()
        return self._parse_project_or_raise(raw)

    def list_project_chats(
        self, project_id: str, cursor: str | None = None, *, limit: int = 30
    ) -> dict[str, Any]:
        raw = self.request(
            "GET",
            urljoin(
                CHATGPT_ORIGIN, f"/backend-api/gizmos/{quote(project_id, safe='')}/conversations"
            ),
            params={"cursor": cursor or "0", "limit": limit, "owned_only": "false"},
        ).json()
        return self._parse_project_chats_page(raw, project_id)

    def list_page(self, offset: int = 0, limit: int = DEFAULT_PAGE_SIZE) -> dict[str, Any]:
        params = self._validate_list_page(offset, limit)
        return self.request(
            "GET", urljoin(CHATGPT_ORIGIN, "/backend-api/conversations"), params=params
        ).json()

    def get_conversation(self, conversation_id: str) -> dict[str, Any]:
        endpoints = self._conversation_endpoints()
        for index, candidate in enumerate(endpoints):
            url = urljoin(
                CHATGPT_ORIGIN, f"/backend-api/{candidate}/{quote(conversation_id, safe='')}"
            )
            try:
                response = self.request("GET", url)
            except RemoteNotFound:
                if index == 0 and len(endpoints) > 1:
                    continue
                raise
            return self._record_conversation_result(candidate, response.json())
        raise ChatGPTError("conversation response was not an object")

    def rename_conversation(self, conversation_id: str, title: str) -> None:
        self.request(
            "PATCH",
            urljoin(CHATGPT_ORIGIN, f"/backend-api/conversation/{quote(conversation_id, safe='')}"),
            json={"title": title},
            expect_json=False,
        )

    def delete_conversation(self, conversation_id: str) -> bool:
        self._validate_conversation_id(conversation_id)
        url = urljoin(
            CHATGPT_ORIGIN, f"/backend-api/conversation/{quote(conversation_id, safe='')}"
        )
        for attempt in (0, 1):
            try:
                response = self.request("PATCH", url, json={"is_visible": False}, expect_json=False)
                break
            except ChatGPTError as error:
                if attempt == 0 and self._is_retryable_gateway_error(error):
                    time.sleep(0.25)
                    continue
                raise
        return self._finalize_delete_conversation(response, conversation_id)

    def move_conversation(self, conversation_id: str, project_id: str | None) -> dict[str, Any]:
        self._validate_move_args(conversation_id, project_id)
        response = self.request(
            "PATCH",
            urljoin(CHATGPT_ORIGIN, f"/backend-api/conversation/{quote(conversation_id, safe='')}"),
            json={"gizmo_id": project_id},
            expect_json=False,
        )
        return self._finalize_move_conversation(response, conversation_id, project_id)

    def _resolve_send_previous(
        self,
        conversation_id: str | None,
        project_id: str | None,
        temporary: bool,
        action: Any,
    ) -> tuple[dict[str, Any] | None, set[str], bool]:
        timing = current_send_timing()
        found, previous, is_pending_branch = self._lookup_local_send_previous(
            conversation_id, temporary, action, timing
        )
        if not found and conversation_id:
            started = time.perf_counter_ns() if timing is not None else None
            previous = self.get_conversation(conversation_id)
            if started is not None:
                timing.record("pre_send_get_conversation_ms", time.perf_counter_ns() - started)
            if timing is not None and action is None:
                timing.increment("pre_send_conversation_cache_miss")
        return self._finalize_send_previous(
            conversation_id, project_id, action, previous, is_pending_branch
        )

    def _edit_parent_id(
        self,
        conversation: dict[str, Any] | None,
        conversation_id: str | None,
        items: list[dict[str, Any]],
        target_index: int,
        target: dict[str, Any],
        message_id: str | None,
    ) -> str:
        parent, url = self._edit_parent_lookup(
            conversation, conversation_id, items, target_index, target, message_id
        )
        if url is None:
            assert parent is not None
            return parent
        assert message_id is not None
        try:
            data = self.request("GET", url).json()
        except (RemoteNotFound, ValueError):
            raise ChatGPTError("The original message parent is unavailable") from None
        return self._extract_edit_parent_from_mapping(data, message_id)

    def _apply_message_action(
        self,
        body: dict[str, Any],
        conversation: dict[str, Any] | None,
        conversation_id: str | None,
        action: Any,
        message_id: str | None,
        prompt: str,
    ) -> str:
        items, target_index, target = self._validate_message_action_target(
            conversation, action, message_id
        )
        if action == MessageAction.RETRY:
            return self._apply_retry_body(body, items, target_index)
        if action == MessageAction.EDIT:
            parent_id = self._edit_parent_id(
                conversation, conversation_id, items, target_index, target, message_id
            )
            return self._apply_edit_body(body, target, parent_id, prompt)
        raise ChatGPTError("Unsupported message action")

    def _create_sentinel_headers_direct(
        self, runtime: SentinelRuntime, timing: SendTiming | None
    ) -> dict[str, str]:
        local_started = time.perf_counter_ns() if timing is not None else None
        initial = runtime.create_initial_proof()
        if local_started is not None:
            timing.record("local_validation_prepare_ms", time.perf_counter_ns() - local_started)
        prepare_started = time.perf_counter_ns() if timing is not None else None
        try:
            requirements = self.request(
                "POST",
                urljoin(CHATGPT_ORIGIN, "/backend-api/sentinel/chat-requirements/prepare"),
                json={"p": initial},
            ).json()
        except Exception as error:
            self._classify_sentinel_prepare_error(error)
        if prepare_started is not None:
            timing.record("sentinel_prepare_http_ms", time.perf_counter_ns() - prepare_started)
        return self._finalize_sentinel_headers(runtime, initial, requirements, timing)

    def create_sentinel_headers(self) -> dict[str, str]:
        timing = current_send_timing()
        self.ensure_auth()
        user_agent = self._sentinel_user_agent()
        with self._sentinel_lock:
            if self._is_closed():
                raise ChatGPTError("ChatGPT client is closed")
            runtime = self._get_or_create_sentinel_runtime(user_agent)
            return self._create_sentinel_headers_direct(runtime, timing)

    def iter_send_prompt(
        self,
        conversation_id: str | None,
        prompt: str,
        attachments: list[dict[str, Any]] | None = None,
        model: str | None = None,
        effort: str | None = None,
        project_id: str | None = None,
        temporary: bool = False,
        action=None,
        message_id: str | None = None,
    ) -> Iterator[tuple[str, Any]]:
        """Stream ChatGPT events directly on the caller thread and yield final snapshot."""
        self._validate_send_options(temporary, project_id)
        previous, previous_ids, is_pending_branch = self._resolve_send_previous(
            conversation_id, project_id, temporary, action
        )
        timing = current_send_timing()
        started = time.perf_counter_ns() if timing is not None else None
        catalog = self.model_catalog()
        if started is not None:
            timing.record("model_catalog_ms", time.perf_counter_ns() - started)
        resolved_model = self._resolve_model_for_send(catalog, previous, model, effort)
        self.ensure_auth()
        conv_id, headers, body, submitted_message_id = self._prepare_send_payload(
            previous, prompt, attachments or []
        )
        if action:
            submitted_message_id = self._apply_message_action(
                body, previous, conv_id, action, message_id, prompt
            )
        submitted_message = self._finalize_send_payload(
            headers,
            body,
            previous,
            submitted_message_id,
            model=resolved_model,
            effort=effort,
            temporary=temporary,
        )
        headers.update(self.create_sentinel_headers())

        sse_started = time.perf_counter_ns() if timing is not None else None
        processor = _ChatGPTSSEProcessor(conv_id, timing)
        try:
            with self._tracked_stream(
                "POST",
                "/backend-api/f/conversation",
                _message=True,
                check_status=False,
                headers=headers,
                json=body,
                timeout=(30.0, 300.0),
            ) as response:
                for pause in _wait_for_stream_head(response):
                    yield "paused", pause
                processor.response_opened = time.perf_counter_ns() if timing is not None else None
                if processor.response_opened is not None and sse_started is not None:
                    timing.record("sse_connect_ms", processor.response_opened - sse_started)
                self._check_stream_response(response)
                decoder = SSEFrameDecoder(max_event_bytes=16 * 1024 * 1024)
                for frame in decoder.decode(response.iter_bytes()):
                    if frame is _STREAM_PAUSED:
                        yield "paused", frame
                        continue
                    done, emitted = processor.feed_frame(frame.data, accepts_bound=True)
                    for item in emitted:
                        yield ("raw", item)
                    if done:
                        break
            resolved_id, streamed_messages = processor.finish()
        except Exception:
            if conversation_id and not temporary and not is_pending_branch:
                self._forget_conversation(conversation_id)
            raise

        if is_pending_branch and conversation_id:
            self._forget_conversation(conversation_id)
        if temporary:
            conversation = self._store_temporary_conversation(
                resolved_id,
                resolved_model,
                prompt,
                previous,
                submitted_message_id,
                submitted_message,
                streamed_messages,
            )
        else:
            conversation = self._try_build_streamed_conversation(
                is_pending_branch=is_pending_branch,
                conversation_id=conversation_id,
                resolved_id=resolved_id,
                action=action,
                previous=previous,
                prompt=prompt,
                model=resolved_model,
                submitted_message_id=submitted_message_id,
                submitted_message=submitted_message,
                streamed_messages=streamed_messages,
            )
            if conversation is None:
                yield "raw", {"type": "response_verifying"}
                started = time.perf_counter_ns() if timing is not None else None
                try:
                    conversation = self.get_conversation(resolved_id)
                except Exception:
                    self._forget_conversation(resolved_id)
                    raise
                finally:
                    if started is not None:
                        timing.record(
                            "post_send_get_conversation_ms", time.perf_counter_ns() - started
                        )
        yield (
            "completed",
            self._finalize_sent_conversation(
                conversation=conversation,
                resolved_id=resolved_id,
                submitted_message_id=submitted_message_id,
                temporary=temporary,
                is_pending_branch=is_pending_branch,
                previous_ids=previous_ids,
            ),
        )

    def send_prompt(
        self,
        conversation_id: str | None,
        prompt: str,
        attachments: list[dict[str, Any]] | None = None,
        on_event: Any = None,
        model: str | None = None,
        effort: str | None = None,
        project_id: str | None = None,
        temporary: bool = False,
        action=None,
        message_id: str | None = None,
    ) -> tuple[str, dict[str, Any], set[str]]:
        result = None
        for kind, value in self.iter_send_prompt(
            conversation_id,
            prompt,
            attachments=attachments,
            model=model,
            effort=effort,
            project_id=project_id,
            temporary=temporary,
            action=action,
            message_id=message_id,
        ):
            if kind == "raw":
                if on_event is not None and (
                    value.get("type") != "bound" or getattr(on_event, "_accepts_bound", False)
                ):
                    on_event(value)
            elif kind == "completed":
                result = value
        assert result is not None
        return result

    def _get_upload_http(self) -> CurlSession:
        existing = getattr(self, "_upload_http", None)
        if (
            existing is not None
            and not getattr(existing, "_closed", False)
            and not getattr(existing, "is_closed", False)
        ):
            return existing
        if self._is_closed():
            raise ChatGPTError("ChatGPT client is closed")
        client = CurlSession(trust_env=False, timeout=30.0, discard_cookies=True)
        self._upload_http = client
        return client

    def _upload_request(self, url: str, **options):
        origin, path = self._validate_upload_url(url)
        transport = HTTPTransport(origin, client_getter=self._get_upload_http)
        try:
            response = transport.send("PUT", path, check_status=False, **options)
        except ChatGPTError:
            raise
        except CurlRequestException:
            raise ChatGPTError("ChatGPT file upload request failed") from None
        if response.status_code >= 400:
            raise ChatGPTError(f"ChatGPT upload returned HTTP {response.status_code}")
        return response

    def _process_project_upload_stream(
        self,
        file_id: str,
        file_name: str,
        mime_type: str,
        project_id: str,
        ensure_active: Callable[[], None],
    ) -> None:
        with self._tracked_stream(
            "POST",
            "/backend-api/files/process_upload_stream",
            check_status=False,
            json=self._project_upload_payload(file_id, file_name, mime_type, project_id),
            timeout=(30.0, 300.0),
        ) as processing:
            self._check_project_upload_stream_response(processing)
            pending = bytearray()
            completed = False
            for chunk in processing.iter_bytes():
                ensure_active()
                if self._feed_project_upload_chunk(pending, chunk):
                    completed = True
                    break
            self._finish_project_upload_buffer(pending, completed)

    def upload_file(
        self, file_name: str, mime_type: str, data, *, project_id=None, cancel_event=None
    ) -> dict[str, Any]:
        path = data if isinstance(data, Path) else None
        stream = path.open("rb") if path is not None else None
        before = os.fstat(stream.fileno()) if stream is not None else None
        try:
            if before is not None and not stat.S_ISREG(before.st_mode):
                raise ChatGPTError("upload source must be a regular file")
            size = before.st_size if before is not None else len(data)
            self._validate_upload_size(file_name, mime_type, size)
            ensure_active = self._make_upload_guard(cancel_event, self._upload_deadline(size))

            def stream_chunks() -> Iterator[bytes]:
                assert stream is not None
                while True:
                    ensure_active()
                    block = stream.read(256 * 1024)
                    if not block:
                        return
                    yield block

            def verify_unchanged() -> None:
                if stream is not None and before is not None:
                    if not same_file_version(before, os.fstat(stream.fileno())):
                        raise ChatGPTError("file changed while it was being uploaded")

            ensure_active()
            if mime_type == "image/png" and project_id is None:
                if stream is not None:
                    header = stream.read(24)
                    stream.seek(0)
                else:
                    header = data[:24]
                ensure_active()
                width, height = self._validate_png_header(header)
                result = self.upload_image(
                    file_name,
                    mime_type,
                    stream_chunks() if stream is not None else data,
                    width,
                    height,
                    size=size,
                    ensure_active=ensure_active,
                )
                ensure_active()
                verify_unchanged()
                return result

            ensure_active()
            slot = self.request(
                "POST",
                urljoin(CHATGPT_ORIGIN, "/backend-api/files"),
                json=self._file_slot_payload(file_name, mime_type, size, project_id),
            ).json()
            ensure_active()
            file_id, upload_url = self._validate_upload_slot(slot, "invalid file upload slot")
            self._upload_request(
                upload_url,
                content=stream_chunks() if stream is not None else data,
                headers=self._upload_blob_headers(mime_type, size, streamed=stream is not None),
                timeout=(30.0, 270.0),
            )
            ensure_active()
            verify_unchanged()
            if project_id:
                self._process_project_upload_stream(
                    file_id, file_name, mime_type, project_id, ensure_active
                )
            else:
                result = self.request(
                    "POST",
                    urljoin(
                        CHATGPT_ORIGIN, f"/backend-api/files/{quote(file_id, safe='')}/uploaded"
                    ),
                    json={},
                ).json()
                if result.get("status") != "success":
                    raise ChatGPTError("provider did not finalize the file upload")
            return {"id": file_id, "name": file_name, "mime_type": mime_type, "size_bytes": size}
        finally:
            if stream is not None:
                stream.close()

    def upload_image(
        self,
        file_name: str,
        mime_type: str,
        data,
        width: int,
        height: int,
        *,
        size: int | None = None,
        ensure_active=None,
    ) -> dict[str, Any]:
        is_bytes, byte_size = self._validate_image_upload(mime_type, data, size)
        slot = self.request(
            "POST",
            urljoin(CHATGPT_ORIGIN, "/backend-api/files"),
            json={"file_name": file_name, "file_size": byte_size, "use_case": "multimodal"},
        ).json()
        if ensure_active is not None:
            ensure_active()
        file_id, upload_url = self._validate_upload_slot(
            slot, "ChatGPT returned an invalid image upload slot"
        )
        self._upload_request(
            upload_url,
            content=data,
            headers=self._upload_blob_headers(mime_type, byte_size, streamed=not is_bytes),
            timeout=(30.0, 120.0),
        )
        if ensure_active is not None:
            ensure_active()
        finalized = self.request(
            "POST",
            urljoin(CHATGPT_ORIGIN, f"/backend-api/files/{quote(file_id, safe='')}/uploaded"),
            json={},
        ).json()
        if finalized.get("status") != "success":
            raise ChatGPTError("ChatGPT did not finalize the image upload")
        return {
            "id": file_id,
            "name": file_name,
            "mime_type": mime_type,
            "size_bytes": byte_size,
            "width": width,
            "height": height,
        }


class ChatGPTProjectEditor(BaseChatGPTProjectEditor):
    """Blocking ChatGPT project controls."""

    def __init__(self, session, factory, *, client_scope=None):
        self.session, self.factory = session, factory
        self.client_scope = client_scope

    @contextmanager
    def _use_client(self):
        if self.client_scope is not None:
            yield self.client_scope()
            return
        client = self.factory(self.session)
        try:
            yield client
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()

    def options(self, project_id=None):
        if not project_id:
            return self._create_options()
        with self._use_client() as client:
            raw = client.get_project(project_id)["metadata"]
        return self._edit_options_from_metadata(raw)

    def update(self, project_id, **values):
        with self._use_client() as client:
            provided = values.get("current")
            state = self._extract_update_state(provided)
            if state is None:
                state = self._extract_update_state(None, client.get_project(project_id))
            assert state is not None
            native, current_name, current_instructions = state
            payload = self._update_payload(native, current_name, current_instructions, values)
            client.request("PATCH", self._project_url(project_id), expect_json=False, json=payload)
            return self._updated_project_result(
                provided, values, current_name, current_instructions
            )

    @classmethod
    def _upload_source(cls, client, project_id, values):
        if values["kind"] == "text":
            name, data, size, last_modified, mime = cls._prepare_text_source(values)
        elif values["kind"] == "file":
            name, data, size, last_modified, mime = cls._prepare_file_source(values["path"])
        else:
            raise ValueError("Unsupported project source type")
        uploaded = client.upload_file(name, mime, data, project_id=project_id)
        return cls._build_uploaded_source_entry(uploaded, name, mime, size, last_modified)

    def add_source(self, project_id, **values):
        (created,) = self.add_sources(project_id, [values])
        return created

    def add_sources(self, project_id, items):
        if not items:
            return []
        with self._use_client() as client:
            uploads = [self._upload_source(client, project_id, item) for item in items]
            ids = self._validate_upload_ids(uploads)
            attached = client.request(
                "POST", self._project_url(project_id) + "/files", json={"files": uploads}
            )
            confirmed = self._confirmed_files(self._extract_attached_rows(attached), ids)
            if confirmed.keys() != ids:
                project = client.get_project(project_id)
                if project.get("id") == project_id:
                    confirmed = self._confirmed_files(project.get("resources"), ids)
            return self._finalize_attached_sources(uploads, confirmed, ids)

    def remove_source(self, project_id, **values):
        with self._use_client() as client:
            rid = values["resource_id"]
            resource = values.get("resource")
            if resource is None or resource.id != rid:
                current = client.get_project(project_id)
                if not any((x.get("file_id") or x.get("id")) == rid for x in current["resources"]):
                    raise ValueError("Resource does not belong to this project")
            client.request(
                "DELETE",
                self._project_url(project_id) + "/files/" + quote(rid, safe=""),
                expect_json=False,
            )
            return self._removed_project_result(values.get("current"), rid)
