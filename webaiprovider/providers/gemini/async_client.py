"""Account-owned asynchronous Gemini HTTPS client, RPC, notebooks, streams, and editor."""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import AsyncGenerator, Awaitable, Callable
from pathlib import Path
from typing import Any

from ...accounts import async_read_session, async_save_session
from ...accounts.credentials import CredentialStore
from ...core.concurrency import gather_bounded
from ...core.file_stat import same_file_version
from ...core.timing import current_send_timing
from ...errors import AccountPaused, RateLimited, WebAIProviderError
from ...resources.message.protocol import ProviderRequest, ProviderStreamEvent
from ...resources.response.async_polling import async_poll_until
from ...transport import AsyncHTTPTransport, AsyncTransport, Transport, TransportConfig
from .base import (
    BASE,
    COOKIE_REFRESH_INTERVAL,
    COOKIE_REFRESH_OPTIONS,
    COOKIE_REFRESH_URL,
    HEADERS,
    MODEL_CATALOG_SIGNATURE,
    ORIGIN,
    BaseGeminiClient,
    BaseGeminiProjectEditor,
)
from .deep_research import (
    build_start_research_fields,
    gemini_tool_fields,
    parse_gemini_research_hnvqhb,
)
from .errors import GeminiError, GeminiStreamError
from .generation import (
    _GeminiResponseProcessor,
    _prepare_upload_specs,
)
from .notebooks import (
    notebook_id,
    parse_notebook_chats,
    parse_notebook_detail,
    parse_notebook_list,
)
from .rpc import RPCPayload, decode_rpc_response, resolve_notebook_rpc_read
from .webchannel import (
    PROCESS_SESSION_PATH,
    WEBCHANNEL_ORIGIN,
    ProcessSessionFrameProcessor,
    WebChannelFrameDecoder,
    WebChannelSessionState,
    build_process_session_headers,
    build_process_session_turn_data,
    extract_handshake_sid,
    initial_rid,
    iter_protobuf_packets,
    new_gsessionid,
)

__all__ = [
    "AsyncGeminiClient",
    "AsyncGeminiNotebooks",
    "AsyncGeminiProjectEditor",
    "AsyncGeminiRPC",
    "async_generate",
    "async_response_events",
    "async_upload_file_references",
    "async_upload_files",
]


class AsyncGeminiClient(BaseGeminiClient):
    """Asynchronous Gemini HTTPS client backed by AsyncHTTPTransport."""

    def __init__(
        self, credential_file: Path | CredentialStore, *, http_factory=None, transport=None
    ):
        self._init_client_state(credential_file, http_factory=http_factory, transport=transport)
        self.transport = transport or AsyncHTTPTransport(
            ORIGIN, client_getter=lambda: self.http, config=TransportConfig(timeout=(10.0, 35.0))
        )
        if self.transport.origin != ORIGIN:
            raise ValueError("Gemini transport must use the Gemini origin")
        self.auth_transport = AsyncHTTPTransport(
            "https://accounts.google.com",
            client_getter=lambda: self.http,
            config=TransportConfig(timeout=(10.0, 35.0)),
        )
        self.upload_transport = AsyncHTTPTransport(
            "https://content-push.googleapis.com",
            config=TransportConfig(timeout=(10.0, 35.0)),
        )
        self.webchannel_transport = AsyncHTTPTransport(
            WEBCHANNEL_ORIGIN,
            client_getter=lambda: self.http,
            config=TransportConfig(timeout=(10.0, 35.0)),
        )
        self.refresh_task = None
        self._save_lock = asyncio.Lock()
        self._activity_lock = asyncio.Lock()

    async def open(self, *, verified_bootstrap: dict[str, str] | None = None):
        from curl_cffi.requests import AsyncSession

        cookies = self._extract_cookies(await async_read_session(self.credential_file))
        self._cookies = dict(cookies)
        if self.http_factory is not None:
            self.http = self.http_factory(
                cookies=cookies, impersonate="chrome", timeout=(10.0, 35.0)
            )
        else:
            from ...transport.network import create_curl_share, curl_cookie_jar

            self.http = AsyncSession(
                cookies=curl_cookie_jar(cookies),
                impersonate="chrome",
                timeout=(10.0, 35.0),
                trust_env=False,
                curl_share=create_curl_share(),
            )
        try:
            if verified_bootstrap is not None:
                self._apply_verified_bootstrap(verified_bootstrap)
            else:
                response = await self.request(
                    "GET", ORIGIN + "/app", headers=HEADERS, allow_redirects=False
                )
                self._check_login_redirect(response)
                AsyncTransport.check(response)
                self._apply_bootstrap_html(response.text)
                await self.persist()
            self.refresh_task = asyncio.create_task(self.refresh(), name="gemini-session-refresh")
        except BaseException:
            await self.close()
            raise
        return self

    async def request(self, method: str, url: str, **options):
        transport, path = self._resolve_request_transport(url)
        response = await transport.send(method, path, check_status=False, **options)
        self._record_rate_limit(transport, response)
        return response

    async def batch(self, payloads, source_path: str = "/app"):
        params, headers, data = self._batch_request_args(payloads, source_path)
        response = await self.request(
            "POST",
            BASE + "batchexecute",
            params=params,
            headers=headers,
            data=data,
            allow_redirects=False,
        )
        AsyncTransport.check(response)
        await self.persist(force=False)
        return response

    async def sync_activity(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if self._is_activity_fresh(now, force=force):
            return
        async with self._activity_lock:
            if self._is_activity_fresh(now, force=force):
                return
            await self.batch([RPCPayload("ESY5D", '[[["bard_activity_enabled"]]]')])
            self._activity_synced_at = now

    async def _graphql_post(self, payload):
        endpoint, xsrf_endpoint, headers, request_options = self._graphql_request_args(payload)
        response = await self.request("POST", endpoint, **request_options)
        refreshed_token = False
        if response.status_code == 400 and self._has_xsrf_error(response):
            token_response = await self.request(
                "GET",
                xsrf_endpoint,
                headers={**HEADERS, "Accept": "text/plain"},
                allow_redirects=False,
            )
            AsyncTransport.check(token_response)
            token = token_response.text.strip()
            if not token:
                raise WebAIProviderError("Gemini session validation token could not be refreshed")
            self.token = token
            refreshed_token = True
            request_options["headers"] = {**headers, "X-Framework-XSRF-Token": token}
            response = await self.request("POST", endpoint, **request_options)
        AsyncTransport.check(response)
        if refreshed_token:
            await self.persist(force=False)
        return response

    async def rename_conversation(self, conversation_id: str, title: str) -> None:
        cleaned, payload, source_path = self._validate_rename(conversation_id, title)
        response = await self.batch([payload], source_path=source_path)
        self._confirm_rename(response.text, conversation_id, cleaned)

    async def discover_models(self):
        response, rpc_response = await asyncio.gather(
            self._graphql_post({"querySignature": MODEL_CATALOG_SIGNATURE}),
            self.batch([RPCPayload("otAQ7b", "[]")]),
        )
        return self._parse_model_catalog_response(response.text, rpc_response.text)

    async def persist(self, *, force: bool = True) -> None:
        async with self._save_lock:
            pending = self._build_persist_record(force=force)
            if pending is None:
                return
            record, now = pending
            try:
                await async_save_session(self.credential_file, record)
            except (OSError, ValueError):
                self._mark_persist_failed()
                return
            self._mark_persisted(record, now)

    async def refresh(self):
        interval = COOKIE_REFRESH_INTERVAL
        failures = 0
        while True:
            await asyncio.sleep(interval)
            try:
                response = await self.request("POST", COOKIE_REFRESH_URL, **COOKIE_REFRESH_OPTIONS)
                if 500 <= response.status_code < 600:
                    failures += 1
                    interval = min(15 * (2 ** min(failures - 1, 6)), 900)
                    continue
                AsyncTransport.check(response)
                await self.persist()
                failures = 0
                interval = COOKIE_REFRESH_INTERVAL
            except asyncio.CancelledError:
                raise
            except AccountPaused:
                return
            except RateLimited as error:
                failures += 1
                interval = min(
                    max(error.retry_after or 15 * (2 ** min(failures - 1, 6)), 1),
                    900,
                )
            except OSError:
                failures += 1
                interval = min(15 * (2 ** min(failures - 1, 6)), 900)
            except WebAIProviderError:
                return
            except Exception:
                return

    async def close(self) -> None:
        try:
            task, self.refresh_task = self.refresh_task, None
            if task and task is not asyncio.current_task():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            http = self.http
            if http is not None:
                try:
                    await self.persist()
                finally:
                    self.http = None
                    self.verified = False
                    await http.close()
        finally:
            self._webchannel_session = None
            try:
                await self.webchannel_transport.close()
            finally:
                try:
                    await self.upload_transport.close()
                finally:
                    if self._provided_transport is not None:
                        await self._provided_transport.close()


class AsyncGeminiRPC:
    """Asynchronous Gemini RPC client."""

    def __init__(self, connect):
        self.connect = connect

    async def call(self, rpc, args, source_path="/notebooks/view", allow_empty=False):
        client = await self.connect()
        response = await client.batch(
            [RPCPayload(rpcid=rpc, payload=json.dumps(args))],
            source_path=source_path,
        )
        if len(getattr(response, "content", b"")) >= 256 * 1024:
            return await asyncio.to_thread(decode_rpc_response, response, rpc, allow_empty)
        return decode_rpc_response(response, rpc, allow_empty)

    async def read(self, mode, *, remote=None, operation="list", cursor=None):
        rpc, args, path, allow_empty = resolve_notebook_rpc_read(
            mode, remote=remote, operation=operation, cursor=cursor
        )
        return await self.call(rpc, args, path, allow_empty=allow_empty)


class AsyncGeminiNotebooks:
    """Asynchronous Gemini notebook reader."""

    def __init__(self, account_id: str):
        self.account_id = account_id
        self.reader: Callable[..., Awaitable[list[Any]]] | None = None

    async def read(self, remote=None, mode=None, cursor=None):
        if self.reader is not None:
            return await self.reader(
                "notebooks",
                remote=notebook_id(remote) if remote else None,
                operation=mode or "list",
                cursor=cursor,
            )
        raise GeminiError("Gemini notebook HTTP reader is not configured")

    async def list(self, cursor=None):
        if cursor:
            raise GeminiError("Gemini notebook pagination is not supported by this version")
        return parse_notebook_list(await self.read(), self.account_id)

    async def chats(self, remote, cursor=None):
        remote = notebook_id(remote)
        return parse_notebook_chats(await self.read(remote, mode="chats", cursor=cursor), remote)

    async def get(self, remote):
        remote = notebook_id(remote)
        return parse_notebook_detail(await self.read(remote), remote, self.account_id)


async def async_upload_files(client, items):
    return [
        [[reference], filename]
        for reference, filename, _ in await async_upload_file_references(client, items)
    ]


async def async_upload_file_references(client, items):
    from curl_cffi import CurlMime

    specs = await asyncio.to_thread(_prepare_upload_specs, items)

    async def upload_one(entry):
        spec, before = entry
        filename, content_type, _size, content, local_path = spec
        mime = CurlMime()
        try:
            mime.addpart(
                name="file",
                content_type=content_type,
                filename=filename,
                **({"data": content} if content is not None else {"local_path": local_path}),
            )
            response = await client.upload_transport.send(
                "POST",
                "/upload",
                headers={
                    "Origin": "https://gemini.google.com",
                    "Referer": "https://gemini.google.com/",
                    "X-Tenant-Id": "bard-storage",
                    "Push-ID": client.push_id,
                },
                multipart=mime,
                impersonate="chrome",
                discard_cookies=True,
                allow_redirects=False,
            )
            if local_path is not None and before is not None:
                after = await asyncio.to_thread(os.stat, local_path)
                if not same_file_version(before, after):
                    raise GeminiError("File changed while it was being uploaded")
            return response.text, filename, content_type
        finally:
            mime.close()

    return await gather_bounded(specs, upload_one, limit=2)


class AsyncGeminiProjectEditor(BaseGeminiProjectEditor):
    """Asynchronous Gemini notebook mutations."""

    def __init__(self, rpc, notebooks):
        self.rpc, self.notebooks = rpc, notebooks

    async def options(self, project_id=None):
        return self._options(project_id)

    async def create(self, **values):
        parts = await self.rpc.call("oMH3Zd", [self._create_detail(values["name"])])
        return self._extract_created_notebook(parts)

    async def delete(self, project_id):
        remote, path = self._notebook_path(project_id)
        return await self.rpc.call("Nwkn9", [remote], path, allow_empty=True)

    async def update(self, project_id, **values):
        remote, path = self._notebook_path(project_id)
        current = values.get("current")
        if self._needs_native_reload(current):
            current = await self.notebooks.get(remote)
        detail = self._update_detail(current, values)
        result = await self.rpc.call("kHv0Vd", [remote, detail], path, allow_empty=True)
        return self._finalize_update(current, result, values)

    async def add_source(self, project_id, **values):
        remote, path = self._notebook_path(project_id)
        kind = values["kind"]
        if kind == "text":
            name, mime_type = values["name"], "text/plain"
            uploaded, text = None, [values["text"], 1]
        elif kind == "file":
            ((reference, name, mime_type),) = await async_upload_file_references(
                await self.rpc.connect(), [values["path"]]
            )
            if not reference.startswith("/"):
                raise WebAIProviderError("Gemini returned an invalid file upload reference")
            uploaded, text = [reference, mime_type], None
        else:
            raise ValueError("Unsupported notebook source type")
        detail = self._build_source_detail(name, mime_type, uploaded, text)
        result = await self.rpc.call("ko3zcd", [remote, detail, [1, 3]], path)
        return self._parse_added_source(
            remote, kind, name, mime_type, result, values.get("existing_resources", ())
        )

    async def remove_source(self, project_id, **values):
        remote, path = self._notebook_path(project_id)
        resource = values["resource_id"]
        self._validate_remove_source(remote, resource)
        result = await self.rpc.call("AptDmf", [resource, [1, 3]], path, allow_empty=True)
        return self._finalize_remove_source(values.get("current"), resource, result)


async def _async_ensure_webchannel_session(client: Any, *, temporary: bool = False) -> Any:
    state = getattr(client, "_webchannel_session", None)
    if state is not None:
        return state
    transport = getattr(client, "webchannel_transport", None) or client.transport
    gsessionid = new_gsessionid()
    rid = initial_rid()
    headers = build_process_session_headers(
        getattr(client, "_cookies", {}),
        temporary=temporary,
        gsessionid=gsessionid,
    )
    resp = await transport.send(
        "POST",
        PROCESS_SESSION_PATH,
        params={"VER": "8", "RID": str(rid), "CVER": "22", "X-HTTP-Session-Id": gsessionid},
        headers=headers,
        data="",
        allow_redirects=False,
    )
    Transport.check(resp)
    decoder = WebChannelFrameDecoder()
    frames = decoder.feed(resp.content or resp.text.encode("utf-8"), final=True)
    sid = extract_handshake_sid(frames)
    if not sid:
        raise GeminiStreamError("Gemini processSession handshake did not return a session ID")
    state = WebChannelSessionState(gsessionid=gsessionid, sid=sid, rid=rid)
    client._webchannel_session = state
    return state


async def async_generate(
    client,
    request,
    metadata,
    notebook=None,
    *,
    files=None,
    request_timeout=(15, 310),
    tool_fields=None,
    raw_override_fields=None,
) -> AsyncGenerator[dict, None]:
    from curl_cffi.const import CurlECode
    from curl_cffi.requests.exceptions import RequestException

    if files is None:
        files = await async_upload_files(client, request.attachments)
    headers, form = build_process_session_turn_data(
        client,
        request,
        metadata,
        files,
        notebook,
        tool_fields=tool_fields,
        raw_override_fields=raw_override_fields,
    )
    transport = getattr(client, "webchannel_transport", None) or client.transport
    decoder = WebChannelFrameDecoder()
    timing = current_send_timing()
    sse_connect_started = time.perf_counter_ns() if timing is not None else None
    processor = ProcessSessionFrameProcessor(request, metadata, timing)
    try:
        for attempt in range(2):
            wc_state = await _async_ensure_webchannel_session(client, temporary=request.temporary)
            submit_params = {
                "VER": "8",
                "gsessionid": wc_state.gsessionid,
                "SID": wc_state.sid,
                "RID": str(wc_state.next_rid()),
                "AID": str(wc_state.aid),
                "t": "1",
            }
            submit_resp = await transport.send(
                "POST",
                PROCESS_SESSION_PATH,
                params=submit_params,
                headers=headers,
                data=form,
                allow_redirects=False,
                timeout=request_timeout,
            )
            if submit_resp.status_code in (400, 404, 410) and attempt == 0:
                client._webchannel_session = None
                continue
            Transport.check(submit_resp)
            break

        stream_params = {
            "gsessionid": wc_state.gsessionid,
            "VER": "8",
            "RID": "rpc",
            "SID": wc_state.sid,
            "AID": str(wc_state.aid),
            "CI": "0",
            "TYPE": "xmlhttp",
        }
        async with transport.stream(
            "GET",
            PROCESS_SESSION_PATH,
            params=stream_params,
            headers=headers,
            allow_redirects=False,
            timeout=request_timeout,
        ) as response:
            processor.response_opened = time.perf_counter_ns() if timing is not None else None
            if sse_connect_started is not None and processor.response_opened is not None:
                timing.record("sse_connect_ms", processor.response_opened - sse_connect_started)
            async for chunk in response.aiter_bytes():
                for batch in decoder.feed(chunk):
                    wc_state.next_aid()
                    for packet in iter_protobuf_packets(batch):
                        for item in processor.feed_packet(packet):
                            yield item
            for batch in decoder.feed(b"", final=True):
                wc_state.next_aid()
                for packet in iter_protobuf_packets(batch):
                    for item in processor.feed_packet(packet):
                        yield item
    except RequestException as error:
        if error.code != CurlECode.OPERATION_TIMEDOUT:
            raise
        raise GeminiStreamError(
            "The Gemini request timed out; it was not retried automatically "
            "and conversation history was preserved."
        ) from None
    processor.verify_complete()
    await client.persist(force=False)


async def async_response_events(
    request: ProviderRequest, *, prepare, request_timeout, media, remember, tool_fields=None
) -> AsyncGenerator[ProviderStreamEvent, None]:
    client, request, metadata, context, action_files = await prepare(request)
    reducer = _GeminiResponseProcessor(request, metadata, media, remember)
    active_tool_fields = (
        tool_fields if tool_fields is not None else gemini_tool_fields(request.tool)
    )
    async for chunk in async_generate(
        client,
        request,
        metadata,
        context,
        files=action_files,
        request_timeout=request_timeout,
        tool_fields=active_tool_fields,
    ):
        for event in reducer.feed_chunk(chunk):
            yield event
    for event in reducer.finish():
        yield event


async def async_start_and_poll_gemini_research(
    client: Any,
    conversation_id: str,
    *,
    turn_token: str | None = None,
    timeout: float = 900.0,
    start_turn: bool = True,
) -> dict[str, Any]:
    """Trigger the Gemini Deep Research execution turn (if needed) and poll hNvQHb until complete."""
    if start_turn:
        fields = build_start_research_fields(conversation_id, turn_token)
        req = ProviderRequest("Araştırmayı başlatın", conversation_id=conversation_id)
        async for _ in async_generate(
            client, req, [conversation_id], files=[], raw_override_fields=fields
        ):
            pass

    async def poll_step() -> dict[str, Any] | None:
        resp = await client.batch([RPCPayload("hNvQHb", f'["{conversation_id}", 10, null, 1]')])
        parsed = decode_rpc_response(resp.text, "hNvQHb")
        return parse_gemini_research_hnvqhb(parsed)

    return await async_poll_until(poll_step, timeout=timeout)
