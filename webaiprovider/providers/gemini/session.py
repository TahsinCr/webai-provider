"""Account-owned Gemini HTTPS session. No browser or third-party Gemini runtime."""

import asyncio
import json
import random
import re
import time
import uuid
from pathlib import Path

from ...accounts import read_session, save_session_async
from ...accounts.credentials import CredentialStore
from ...errors import AccountPaused, ProviderError, RateLimited
from ...resources.model.model import Model
from ...transport import AsyncRequestTransport, AsyncTransport, TransportConfig

ORIGIN = "https://gemini.google.com"
BASE = ORIGIN + "/_/BardChatUi/data/"
HEADERS = {"Origin": ORIGIN, "Referer": ORIGIN + "/", "X-Same-Domain": "1"}
GRAPHQL_PATH = "/_/BardChatUi/"
GRAPHQL_SCHEMA = "GEMINI_WEB_GRAPHQL"
RENAME_CONVERSATION_SIGNATURE = "2/AK6lUKXaLqlYDDOn9zA0qKcCFN+eXBsWI9SXBtcRr1o="
MODEL_CATALOG_SIGNATURE = "2/3fx8hkya5YwhXIcIHgqPDyS+UZ/xU43IKaBmI1FKsqo="


class GeminiSession:
    def __init__(
        self, credential_file: Path | CredentialStore, *, http_factory=None, transport=None
    ):
        self.credential_file = credential_file
        self.http_factory = http_factory
        self._provided_transport = transport
        self.http = None
        self.transport = transport or AsyncRequestTransport(
            ORIGIN, client_getter=lambda: self.http, config=TransportConfig(timeout=(10.0, 35.0))
        )
        if self.transport.origin != ORIGIN:
            raise ValueError("Gemini transport must use the Gemini origin")
        self.auth_transport = AsyncRequestTransport(
            "https://accounts.google.com",
            client_getter=lambda: self.http,
            config=TransportConfig(timeout=(10.0, 35.0)),
        )
        self.upload_transport = AsyncRequestTransport(
            "https://content-push.googleapis.com",
            client_getter=lambda: self.http,
            config=TransportConfig(timeout=(10.0, 35.0)),
        )
        self.refresh_task = None
        self.language = "en"
        self.push_id = ""
        self.build = self.sid = self.token = ""
        self.rpc_path_prefix = ""
        self.request_id = random.randint(10000, 99999)
        self.session_id = str(uuid.uuid4()).upper()
        self.models: tuple[Model, ...] = ()
        self._activity_synced_at: float | None = None
        self._saved = None
        self.verified = False
        self.persistence_warning = None
        self._save_lock = asyncio.Lock()

    async def open(self):
        from curl_cffi.requests import AsyncSession

        saved = read_session(self.credential_file)
        raw_cookies = saved.get("cookies")
        if not isinstance(raw_cookies, dict):
            raw_cookies = saved
        cookies = {
            key: value
            for key, value in raw_cookies.items()
            if key not in {"token", "build", "sid", "language", "push_id", "rpc_path_prefix"}
        }
        if not cookies.get("__Secure-1PSID"):
            raise AccountPaused("Gemini session cookies are missing")
        self.http = (self.http_factory or AsyncSession)(
            cookies=cookies, impersonate="chrome145", timeout=(10.0, 35.0)
        )
        try:
            response = await self.request(
                "GET", ORIGIN + "/app", headers=HEADERS, allow_redirects=False
            )
            AsyncTransport.check(response)
            fields = {}
            for name in ("SNlM0e", "cfb2h", "FdrFJe", "TuX5cc", "qKIAYe", "eptZe"):
                match = re.search(r'"' + name + r'"\s*:\s*"([^"\\]*)"', response.text)
                fields[name] = match.group(1) if match else ""
            self.token, self.build, self.sid = (fields[k] for k in ("SNlM0e", "cfb2h", "FdrFJe"))
            self.language = fields["TuX5cc"] or "en"
            self.push_id = fields["qKIAYe"]
            self.rpc_path_prefix = fields["eptZe"]
            if not self.token:
                raise AccountPaused("Gemini session is invalid; verify the account")
            self.verified = True
            self._activity_synced_at = time.monotonic()
            await self.persist()
            self.refresh_task = asyncio.create_task(self.refresh(), name="gemini-session-refresh")
        except BaseException:
            await self.close()
            raise
        return self

    async def request(self, method, url, **options):
        from urllib.parse import urlsplit

        target = urlsplit(url)
        origin = f"{target.scheme}://{target.netloc}"
        transports = {
            self.transport.origin: self.transport,
            self.auth_transport.origin: self.auth_transport,
        }
        transport = transports.get(origin)
        if transport is None:
            raise ValueError("Gemini request origin is not allowed")
        path = target.path + ("?" + target.query if target.query else "")
        return await transport.send(method, path, check_status=False, **options)

    def params(self):
        current = self.request_id
        self.request_id += 100000
        return {
            "hl": self.language,
            "_reqid": current,
            "rt": "c",
            "bl": self.build,
            "f.sid": self.sid,
        }

    async def batch(self, payloads, source_path="/app"):
        params = self.params()
        params.update({"rpcids": ",".join(p.rpcid for p in payloads), "source-path": source_path})
        selector = [
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            [4, 5, 6, 8],
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            self.session_id,
        ]
        response = await self.request(
            "POST",
            BASE + "batchexecute",
            params=params,
            headers={
                **HEADERS,
                "x-goog-ext-525001261-jspb": json.dumps(selector),
                "x-goog-ext-73010989-jspb": "[0]",
            },
            data={"at": self.token, "f.req": json.dumps([[p.serialize() for p in payloads]])},
            allow_redirects=False,
        )
        AsyncTransport.check(response)
        await self.persist()
        return response

    async def sync_activity(self, *, force=False):
        """Mark the authenticated web session active before model generation."""
        now = time.monotonic()
        if (
            not force
            and self._activity_synced_at is not None
            and now - self._activity_synced_at < 300
        ):
            return
        from .rpc import RPCPayload

        await self.batch([RPCPayload("ESY5D", '[[["bard_activity_enabled"]]]')])
        self._activity_synced_at = now

    def graphql_endpoints(self):
        prefix = self.rpc_path_prefix
        marker = GRAPHQL_PATH
        if not prefix.startswith("/") or marker not in prefix:
            raise ProviderError("Gemini web query endpoint is unavailable")
        base = ORIGIN + prefix[: prefix.index(marker)] + marker + "graphql"
        return (
            f"{base}/schemas/{GRAPHQL_SCHEMA}/executeQuery",
            f"{base}/xsrfToken",
        )

    async def rename_conversation(self, conversation_id, title):
        if not isinstance(title, str):
            raise ValueError("Gemini chat title must be text")
        title = title.strip()
        if not conversation_id or not 1 <= len(title) <= 100:
            raise ValueError("Gemini chat title must contain 1–100 characters")
        if self.http is None or not self.token:
            raise AccountPaused("Gemini session is not ready; reverify the account")

        endpoint, xsrf_endpoint = self.graphql_endpoints()
        headers = {
            **HEADERS,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Framework-XSRF-Token": self.token,
        }
        body = json.dumps(
            {
                "querySignature": RENAME_CONVERSATION_SIGNATURE,
                "variables": {"conversationId": conversation_id, "title": title},
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        response = await self.request(
            "POST",
            endpoint,
            headers=headers,
            data=body,
            allow_redirects=False,
        )
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
                raise ProviderError("Gemini session validation token could not be refreshed")
            self.token = token
            response = await self.request(
                "POST",
                endpoint,
                headers={**headers, "X-Framework-XSRF-Token": token},
                data=body,
                allow_redirects=False,
            )
        AsyncTransport.check(response)
        result = self._decode_graphql(response.text)
        errors = result.get("errors")
        if errors:
            message = errors[0].get("message") if isinstance(errors[0], dict) else None
            raise ProviderError(message or "Gemini conversation could not be renamed")
        data = result.get("data")
        update = data.get("updateConversation") if isinstance(data, dict) else None
        conversation = update.get("clientConversation") if isinstance(update, dict) else None
        if (
            not isinstance(conversation, dict)
            or conversation.get("conversationId") != conversation_id
            or conversation.get("title") != title
        ):
            raise ProviderError("Gemini conversation rename could not be confirmed by the server")
        await self.persist()

    @staticmethod
    def _has_xsrf_error(response):
        try:
            payload = response.json()
        except (ValueError, TypeError):
            return False
        if not isinstance(payload, list):
            return False
        return any(
            isinstance(item, dict)
            and isinstance(item.get("error"), dict)
            and isinstance(item["error"].get("details"), list)
            and any(
                isinstance(detail, dict)
                and detail.get("@type") == "type.googleapis.com/fava.request.XsrfData"
                for detail in item["error"]["details"]
            )
            for item in payload
        )

    @staticmethod
    def _decode_graphql(text):
        payload = text.lstrip()
        if payload.startswith(")]}'"):
            payload = payload[4:].lstrip("\r\n")
        try:
            results = json.loads(payload)
        except (ValueError, TypeError) as error:
            raise ProviderError("Gemini web query response could not be parsed") from error
        if not isinstance(results, list):
            raise ProviderError("Gemini web query response format has changed")
        for result in results:
            if isinstance(result, dict) and isinstance(result.get("data"), dict):
                return result
            if isinstance(result, dict) and result.get("errors"):
                return result
        raise ProviderError("Gemini web query response is incomplete")

    async def discover_models(self):
        from .catalogs import parse_model_catalog
        from .rpc import RPCPayload

        endpoint, xsrf_endpoint = self.graphql_endpoints()
        headers = {
            **HEADERS,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Framework-XSRF-Token": self.token,
        }
        body = json.dumps(
            {"querySignature": MODEL_CATALOG_SIGNATURE},
            ensure_ascii=False,
            separators=(",", ":"),
        )

        async def fetch_graphql():
            res = await self.request(
                "POST",
                endpoint,
                headers=headers,
                data=body,
                allow_redirects=False,
            )
            if res.status_code == 400 and self._has_xsrf_error(res):
                token_response = await self.request(
                    "GET",
                    xsrf_endpoint,
                    headers={**HEADERS, "Accept": "text/plain"},
                    allow_redirects=False,
                )
                AsyncTransport.check(token_response)
                token = token_response.text.strip()
                if not token:
                    raise ProviderError("Gemini session validation token could not be refreshed")
                self.token = token
                res = await self.request(
                    "POST",
                    endpoint,
                    headers={**headers, "X-Framework-XSRF-Token": token},
                    data=body,
                    allow_redirects=False,
                )
            AsyncTransport.check(res)
            return res

        response, rpc_response = await asyncio.gather(
            fetch_graphql(),
            self.batch([RPCPayload("otAQ7b", "[]")]),
        )
        result = self._decode_graphql(response.text)
        data = result.get("data") or {}
        viewer = data.get("viewer") or {}
        status = (viewer.get("rpcs") or {}).get("getUserStatus")
        return parse_model_catalog(status, rpc_response.text)

    async def persist(self):
        async with self._save_lock:
            if self.http is None or not self.verified:
                return
            cookies = self.http.cookies.get_dict()
            record = dict(cookies)
            if self.token:
                record.update(
                    token=self.token,
                    build=self.build,
                    sid=self.sid,
                    language=self.language,
                    push_id=self.push_id,
                    rpc_path_prefix=self.rpc_path_prefix,
                )
            if record != self._saved:
                try:
                    await save_session_async(self.credential_file, record)
                except (OSError, ValueError):
                    self.persistence_warning = "Could not update the Gemini session record."
                    return
                self._saved = record
                self.persistence_warning = None

    async def refresh(self):
        interval = 600
        failures = 0
        while True:
            await asyncio.sleep(interval)
            try:
                response = await self.request(
                    "POST",
                    "https://accounts.google.com/RotateCookies",
                    headers={
                        "Origin": "https://accounts.google.com",
                        "Content-Type": "application/json",
                    },
                    data='[000,"-0000000000000000000"]',
                    allow_redirects=False,
                )
                if 500 <= response.status_code < 600:
                    failures += 1
                    interval = min(15 * (2 ** min(failures - 1, 6)), 900)
                    continue
                AsyncTransport.check(response)
                await self.persist()
                failures = 0
                interval = 600
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
            except ProviderError:
                return
            except Exception:
                return

    async def close(self):
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
            if self._provided_transport is not None:
                await self._provided_transport.close()
