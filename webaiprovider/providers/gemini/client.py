"""Account-owned blocking Gemini HTTPS client and notebook editor."""

from __future__ import annotations

import threading
import time
from pathlib import Path

from ...accounts import read_session, save_session
from ...accounts.credentials import CredentialStore
from ...errors import ProviderError
from ...transport import HTTPTransport, Transport, TransportConfig
from .base import (
    BASE,
    HEADERS,
    MODEL_CATALOG_SIGNATURE,
    ORIGIN,
    BaseGeminiClient,
    BaseGeminiProjectEditor,
)
from .generation import upload_file_references_sync
from .rpc import RPCPayload

__all__ = [
    "GeminiClient",
    "GeminiProjectEditor",
]


class GeminiClient(BaseGeminiClient):
    """Blocking Gemini HTTPS client running directly on the caller thread."""

    def __init__(
        self, credential_file: Path | CredentialStore, *, http_factory=None, transport=None
    ):
        self._init_client_state(credential_file, http_factory=http_factory, transport=transport)
        self.transport = transport or HTTPTransport(
            ORIGIN, client_getter=lambda: self.http, config=TransportConfig(timeout=(10.0, 35.0))
        )
        if self.transport.origin != ORIGIN:
            raise ValueError("Gemini transport must use the Gemini origin")
        self.auth_transport = HTTPTransport(
            "https://accounts.google.com",
            client_getter=lambda: self.http,
            config=TransportConfig(timeout=(10.0, 35.0)),
        )
        self.upload_transport = HTTPTransport(
            "https://content-push.googleapis.com",
            config=TransportConfig(timeout=(10.0, 35.0)),
        )
        self._save_lock = threading.Lock()
        self._activity_lock = threading.Lock()

    def open(self):
        from curl_cffi.requests import Session

        cookies = self._extract_cookies(read_session(self.credential_file))
        if self.http_factory is not None:
            self.http = self.http_factory(
                cookies=cookies, impersonate="chrome", timeout=(10.0, 35.0)
            )
        else:
            from ...transport.network import curl_cookie_jar

            self.http = Session(
                cookies=curl_cookie_jar(cookies),
                impersonate="chrome",
                timeout=(10.0, 35.0),
                trust_env=False,
            )
        try:
            response = self.request("GET", ORIGIN + "/app", headers=HEADERS, allow_redirects=False)
            self._check_login_redirect(response)
            Transport.check(response)
            self._apply_bootstrap_html(response.text)
            self.persist()
        except BaseException:
            self.close()
            raise
        return self

    def request(self, method: str, url: str, **options):
        transport, path = self._resolve_request_transport(url)
        response = transport.send(method, path, check_status=False, **options)
        self._record_rate_limit(transport, response)
        return response

    def batch(self, payloads, source_path: str = "/app"):
        params, headers, data = self._batch_request_args(payloads, source_path)
        response = self.request(
            "POST",
            BASE + "batchexecute",
            params=params,
            headers=headers,
            data=data,
            allow_redirects=False,
        )
        Transport.check(response)
        self.persist(force=False)
        return response

    def sync_activity(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if self._is_activity_fresh(now, force=force):
            return
        with self._activity_lock:
            if self._is_activity_fresh(now, force=force):
                return
            self.batch([RPCPayload("ESY5D", '[[["bard_activity_enabled"]]]')])
            self._activity_synced_at = now

    def _graphql_post(self, payload):
        endpoint, xsrf_endpoint, headers, request_options = self._graphql_request_args(payload)
        response = self.request("POST", endpoint, **request_options)
        refreshed_token = False
        if response.status_code == 400 and self._has_xsrf_error(response):
            token_response = self.request(
                "GET",
                xsrf_endpoint,
                headers={**HEADERS, "Accept": "text/plain"},
                allow_redirects=False,
            )
            Transport.check(token_response)
            token = token_response.text.strip()
            if not token:
                raise ProviderError("Gemini session validation token could not be refreshed")
            self.token = token
            refreshed_token = True
            request_options["headers"] = {**headers, "X-Framework-XSRF-Token": token}
            response = self.request("POST", endpoint, **request_options)
        Transport.check(response)
        if refreshed_token:
            self.persist(force=False)
        return response

    def rename_conversation(self, conversation_id: str, title: str) -> None:
        cleaned, payload, source_path = self._validate_rename(conversation_id, title)
        response = self.batch([payload], source_path=source_path)
        self._confirm_rename(response.text, conversation_id, cleaned)

    def discover_models(self):
        response = self._graphql_post({"querySignature": MODEL_CATALOG_SIGNATURE})
        rpc_response = self.batch([RPCPayload("otAQ7b", "[]")])
        return self._parse_model_catalog_response(response.text, rpc_response.text)

    def persist(self, *, force: bool = True) -> None:
        with self._save_lock:
            pending = self._build_persist_record(force=force)
            if pending is None:
                return
            record, now = pending
            try:
                save_session(self.credential_file, record)
            except (OSError, ValueError):
                self._mark_persist_failed()
                return
            self._mark_persisted(record, now)

    def close(self) -> None:
        try:
            http = self.http
            if http is not None:
                try:
                    self.persist()
                finally:
                    self.http = None
                    self.verified = False
                    http.close()
        finally:
            try:
                self.upload_transport.close()
            finally:
                if self._provided_transport is not None:
                    self._provided_transport.close()


class GeminiProjectEditor(BaseGeminiProjectEditor):
    """Blocking Gemini notebook mutations."""

    def __init__(self, rpc, notebooks):
        self.rpc, self.notebooks = rpc, notebooks

    def options(self, project_id=None):
        return self._options(project_id)

    def create(self, **values):
        parts = self.rpc.call("oMH3Zd", [self._create_detail(values["name"])])
        return self._extract_created_notebook(parts)

    def delete(self, project_id):
        remote, path = self._notebook_path(project_id)
        return self.rpc.call("Nwkn9", [remote], path, allow_empty=True)

    def update(self, project_id, **values):
        remote, path = self._notebook_path(project_id)
        current = values.get("current")
        if self._needs_native_reload(current):
            current = self.notebooks.get(remote)
        detail = self._update_detail(current, values)
        result = self.rpc.call("kHv0Vd", [remote, detail], path, allow_empty=True)
        return self._finalize_update(current, result, values)

    def add_source(self, project_id, **values):
        remote, path = self._notebook_path(project_id)
        kind = values["kind"]
        if kind == "text":
            name, mime_type = values["name"], "text/plain"
            uploaded, text = None, [values["text"], 1]
        elif kind == "file":
            ((reference, name, mime_type),) = upload_file_references_sync(
                self.rpc.connect(), [values["path"]]
            )
            if not reference.startswith("/"):
                raise ProviderError("Gemini returned an invalid file upload reference")
            uploaded, text = [reference, mime_type], None
        else:
            raise ValueError("Unsupported notebook source type")
        detail = self._build_source_detail(name, mime_type, uploaded, text)
        result = self.rpc.call("ko3zcd", [remote, detail, [1, 3]], path)
        return self._parse_added_source(
            remote, kind, name, mime_type, result, values.get("existing_resources", ())
        )

    def remove_source(self, project_id, **values):
        remote, path = self._notebook_path(project_id)
        resource = values["resource_id"]
        self._validate_remove_source(remote, resource)
        result = self.rpc.call("AptDmf", [resource, [1, 3]], path, allow_empty=True)
        return self._finalize_remove_source(values.get("current"), resource, result)
