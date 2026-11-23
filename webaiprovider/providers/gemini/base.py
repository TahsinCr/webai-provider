from __future__ import annotations

import json
import random
import re
import time
import uuid
from dataclasses import replace
from typing import Any
from urllib.parse import urlsplit

from ...accounts import session_exists
from ...accounts.status import SessionState, SessionStatus
from ...base import BaseWebAIProvider, Capability, WebAIProviderType
from ...core.json import json_dumps, json_loads
from ...errors import AccountPaused, WebAIProviderError
from ...resources.chat.types import Conversation
from ...resources.message.attachment import Attachment
from ...resources.message.types import MessageAction
from ...resources.project.types import ProjectData, ProjectOptions
from ...transport.downloader import DownloadSource, NativeDownloadClient, cookie_header
from ...transport.retry import parse_retry_after
from .errors import GeminiCompatibilityError, GeminiSignInRequired
from .history import action_turn_index, parse_chat_history
from .media import PLACEHOLDER, valid_media_url
from .notebooks import field, notebook_id
from .rpc import RPCPayload, rpc_parts, rpc_value

ORIGIN = "https://gemini.google.com"
BASE = ORIGIN + "/_/BardChatUi/data/"
HEADERS = {"Origin": ORIGIN, "Referer": ORIGIN + "/", "X-Same-Domain": "1"}
COOKIE_REFRESH_INTERVAL = 600
COOKIE_REFRESH_URL = "https://accounts.google.com/RotateCookies"
COOKIE_REFRESH_OPTIONS = {
    "headers": {"Origin": "https://accounts.google.com", "Content-Type": "application/json"},
    "data": '[000,"-0000000000000000000"]',
    "allow_redirects": False,
}
GRAPHQL_PATH = "/_/BardChatUi/"
GRAPHQL_SCHEMA = "GEMINI_WEB_GRAPHQL"
MODEL_CATALOG_SIGNATURE = "2/3fx8hkya5YwhXIcIHgqPDyS+UZ/xU43IKaBmI1FKsqo="
_BOOTSTRAP_FIELDS = ("SNlM0e", "cfb2h", "FdrFJe", "TuX5cc", "qKIAYe", "eptZe")
_BOOTSTRAP_FIELD_RE = re.compile(
    r'"(' + "|".join(_BOOTSTRAP_FIELDS) + r')"\s*:\s*("(?:[^"\\]|\\.)*")'
)
_SECONDARY_PERSIST_INTERVAL = 5.0
_CHAT_METADATA_TTL_SECONDS = 1800.0
_CRITICAL_PERSIST_FIELDS = (
    "OSID",
    "__Secure-OSID",
    "__Secure-1PSID",
    "__Secure-1PSIDTS",
    "__Secure-1PSIDCC",
    "__Secure-3PSIDTS",
    "__Secure-3PSIDCC",
    "SIDCC",
    "token",
    "build",
    "sid",
    "push_id",
    "rpc_path_prefix",
)


class BaseGeminiClient:
    """Shared non-I/O session, batch, GraphQL, and cookie logic for Gemini clients."""

    ORIGIN = ORIGIN
    BASE = BASE
    HEADERS = HEADERS

    def _init_client_state(self, credential_file: Any, *, http_factory=None, transport=None):
        self.credential_file = credential_file
        self.http_factory = http_factory
        self._provided_transport = transport
        self.http: Any = None
        self.language = "en"
        self.push_id = ""
        self.build = self.sid = self.token = ""
        self.rpc_path_prefix = ""
        self.request_id = random.randint(10000, 99999)
        self.session_id = str(uuid.uuid4()).upper()
        self.models: tuple[Any, ...] = ()
        self._activity_synced_at: float | None = None
        self._saved: dict[str, Any] | None = None
        self.verified = False
        self.persistence_warning: str | None = None
        self._last_persisted_at: float | None = None
        self._dirty = False

    @staticmethod
    def _extract_cookies(saved: dict[str, Any]) -> dict[str, Any]:
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
        return cookies

    @staticmethod
    def _check_login_redirect(response: Any) -> None:
        if 300 <= response.status_code < 400:
            location = response.headers.get("location", "")
            if isinstance(location, str) and (
                location.partition("?")[0].rstrip("/").casefold()
                == "https://accounts.google.com/servicelogin"
            ):
                raise GeminiSignInRequired(
                    "Google requires sign-in to Gemini. Sign in again and reimport the account."
                )

    def _apply_bootstrap_html(self, text: str) -> None:
        fields: dict[str, str] = {}
        for match in _BOOTSTRAP_FIELD_RE.finditer(text):
            name = match.group(1)
            if name not in fields:
                raw = match.group(2)
                if "\\" in raw:
                    try:
                        fields[name] = json_loads(raw)
                    except ValueError:
                        continue
                else:
                    fields[name] = raw[1:-1]
                if len(fields) == len(_BOOTSTRAP_FIELDS):
                    break
        self.token, self.build, self.sid = (
            fields.get(k, "") for k in ("SNlM0e", "cfb2h", "FdrFJe")
        )
        self.language = fields.get("TuX5cc") or "en"
        self.push_id = fields.get("qKIAYe", "")
        self.rpc_path_prefix = fields.get("eptZe", "")
        if not self.token:
            raise AccountPaused("Gemini session is invalid; verify the account")
        self.verified = True
        self._activity_synced_at = time.monotonic()

    def _export_bootstrap(self) -> dict[str, str]:
        return {
            "token": self.token,
            "build": self.build,
            "sid": self.sid,
            "language": self.language,
            "push_id": self.push_id,
            "rpc_path_prefix": self.rpc_path_prefix,
        }

    def _apply_verified_bootstrap(self, bootstrap: dict[str, str]) -> None:
        self.token = bootstrap.get("token", "")
        self.build = bootstrap.get("build", "")
        self.sid = bootstrap.get("sid", "")
        self.language = bootstrap.get("language") or "en"
        self.push_id = bootstrap.get("push_id", "")
        self.rpc_path_prefix = bootstrap.get("rpc_path_prefix", "")
        if not self.token:
            raise AccountPaused("Gemini session is invalid; verify the account")
        self.verified = True
        self._activity_synced_at = time.monotonic()

    def params(self) -> dict[str, Any]:
        current = self.request_id
        self.request_id += 100000
        return {
            "hl": self.language,
            "_reqid": current,
            "rt": "c",
            "bl": self.build,
            "f.sid": self.sid,
        }

    def _batch_request_args(self, payloads: list[RPCPayload], source_path: str = "/app"):
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
        headers = {
            **HEADERS,
            "x-goog-ext-525001261-jspb": json_dumps(selector),
            "x-goog-ext-73010989-jspb": "[0]",
        }
        data = {"at": self.token, "f.req": json_dumps([[p.serialize() for p in payloads]])}
        return params, headers, data

    def graphql_endpoints(self) -> tuple[str, str]:
        prefix = self.rpc_path_prefix
        marker = GRAPHQL_PATH
        if not prefix.startswith("/") or marker not in prefix:
            raise WebAIProviderError("Gemini web query endpoint is unavailable")
        base = ORIGIN + prefix[: prefix.index(marker)] + marker + "graphql"
        return (
            f"{base}/schemas/{GRAPHQL_SCHEMA}/executeQuery",
            f"{base}/xsrfToken",
        )

    @staticmethod
    def _has_xsrf_error(response: Any) -> bool:
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
    def _decode_graphql(text: str) -> dict[str, Any]:
        payload = text.lstrip()
        if payload.startswith(")]}'"):
            payload = payload[4:].lstrip("\r\n")
        try:
            results = json_loads(payload)
        except (ValueError, TypeError) as error:
            raise WebAIProviderError("Gemini web query response could not be parsed") from error
        if not isinstance(results, list):
            raise WebAIProviderError("Gemini web query response format has changed")
        for result in results:
            if isinstance(result, dict) and isinstance(result.get("data"), dict):
                return result
            if isinstance(result, dict) and result.get("errors"):
                return result
        raise WebAIProviderError("Gemini web query response is incomplete")

    def _validate_rename(self, conversation_id: Any, title: Any) -> tuple[str, RPCPayload, str]:
        if not isinstance(title, str):
            raise ValueError("Gemini chat title must be text")
        cleaned = title.strip()
        if not conversation_id or not 1 <= len(cleaned) <= 100:
            raise ValueError("Gemini chat title must contain 1–100 characters")
        if self.http is None or not self.token:
            raise AccountPaused("Gemini session is not ready; reverify the account")
        payload = RPCPayload(
            "MUAZcd",
            json.dumps(
                [None, [["title"]], [conversation_id, cleaned]],
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        )
        return cleaned, payload, "/app/" + conversation_id.removeprefix("c_")

    @staticmethod
    def _confirm_rename(response_text: str, conversation_id: str, title: str) -> None:
        parts = list(rpc_parts(response_text, "MUAZcd"))
        row = rpc_value(parts[0], 1) if parts else None
        if rpc_value(row, 0) != conversation_id or rpc_value(row, 1) != title:
            raise WebAIProviderError(
                "Gemini conversation rename could not be confirmed by the server"
            )

    def _resolve_request_transport(self, url: str) -> tuple[Any, str]:
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
        return transport, path

    @staticmethod
    def _record_rate_limit(transport: Any, response: Any) -> None:
        if response.status_code == 429:
            record = getattr(transport, "record_rate_limit", None)
            if callable(record):
                record(parse_retry_after(response.headers.get("retry-after"), now=time.time()))

    def _is_activity_fresh(self, now: float, *, force: bool) -> bool:
        return (
            not force
            and self._activity_synced_at is not None
            and now - self._activity_synced_at < 300
        )

    def _graphql_request_args(
        self, payload: Any
    ) -> tuple[str, str, dict[str, str], dict[str, Any]]:
        endpoint, xsrf_endpoint = self.graphql_endpoints()
        headers = {
            **HEADERS,
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Framework-XSRF-Token": self.token,
        }
        options = {"headers": headers, "data": json_dumps(payload), "allow_redirects": False}
        return endpoint, xsrf_endpoint, headers, options

    def _parse_model_catalog_response(self, graphql_text: str, rpc_text: str):
        from .catalogs import parse_model_catalog

        result = self._decode_graphql(graphql_text)
        data = result.get("data") or {}
        viewer = data.get("viewer") or {}
        status = (viewer.get("rpcs") or {}).get("getUserStatus")
        return parse_model_catalog(status, rpc_text)

    def _mark_persisted(self, record: dict[str, Any], now: float) -> None:
        self._saved = record
        self._last_persisted_at = now
        self._dirty = False
        self.persistence_warning = None

    def _mark_persist_failed(self) -> None:
        self.persistence_warning = "Could not update the Gemini session record."

    def _build_persist_record(self, *, force: bool) -> tuple[dict[str, Any], float] | None:
        if self.http is None or not self.verified:
            return None
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
        if record == self._saved:
            self._dirty = False
            return None
        now = time.monotonic()
        critical_changed = self._saved is None or any(
            record.get(key) != self._saved.get(key) for key in _CRITICAL_PERSIST_FIELDS
        )
        if (
            not force
            and not critical_changed
            and self._last_persisted_at is not None
            and now - self._last_persisted_at < _SECONDARY_PERSIST_INTERVAL
        ):
            self._dirty = True
            return None
        return record, now


class BaseGeminiProjectEditor:
    """Shared non-I/O notebook mutation builders and validators for Gemini editors."""

    @staticmethod
    def _notebook_path(project_id: str) -> tuple[str, str]:
        remote = notebook_id(project_id)
        return remote, "/notebook/" + remote.split("/")[1]

    @staticmethod
    def _needs_native_reload(current: Any) -> bool:
        return current is None or not isinstance(
            (current.metadata or {}).get("native"), (list, tuple)
        )

    @staticmethod
    def _finalize_update(current: ProjectData, result: Any, values: dict[str, Any]) -> Any:
        if isinstance(values.get("current"), ProjectData):
            return replace(
                current,
                name=values.get("name", current.name),
                instructions=values.get("instructions", current.instructions or ""),
            )
        return result

    @staticmethod
    def _build_source_detail(name: str, mime_type: str, uploaded: Any, text: Any) -> list[Any]:
        return [None, name, None, None, uploaded, text, None, None, mime_type]

    @staticmethod
    def _validate_remove_source(remote: str, resource: str) -> None:
        if not resource.startswith(remote + "/sources/"):
            raise ValueError("Source belongs to another notebook")

    @staticmethod
    def _finalize_remove_source(current: Any, resource: str, result: Any) -> Any:
        if isinstance(current, ProjectData):
            return replace(
                current,
                resources=tuple(r for r in current.resources if r.id != resource),
            )
        return result

    @staticmethod
    def _options(project_id: str | None = None) -> ProjectOptions:
        return ProjectOptions(
            fields=("name", "instructions") if project_id else ("name",),
            sources=(
                ({"id": "text", "title": "Text"}, {"id": "file", "title": "File"})
                if project_id
                else ()
            ),
            remove_sources=bool(project_id),
        )

    @staticmethod
    def _create_detail(name: str) -> list[Any]:
        return [
            name,
            "",
            None,
            None,
            None,
            None,
            None,
            None,
            0,
            None,
            1,
            None,
            None,
            None,
            None,
            None,
            [2, None, None, None, 1],
        ]

    @staticmethod
    def _extract_created_notebook(parts: Any) -> str:
        matches: list[str] = []

        def collect(item):
            if not isinstance(item, list):
                return
            if len(item) == 2 and isinstance(item[0], str) and item[0].startswith("notebooks/"):
                matches.append(notebook_id(item[0]))
            else:
                for child in item:
                    collect(child)

        collect(parts)
        if len(set(matches)) != 1:
            raise WebAIProviderError(
                "Notebook creation could not be confirmed; the operation was not retried"
            )
        return matches[0]

    @staticmethod
    def _update_detail(current: ProjectData, values: dict[str, Any]) -> list[Any]:
        native = current.metadata["native"][1]
        if len(native) < 13 or not isinstance(native[12], list):
            raise WebAIProviderError("Gemini notebook settings contract changed")
        return [
            values.get("name", current.name),
            current.metadata.get("description") or "",
            values.get("instructions", current.instructions or ""),
            None,
            None,
            None,
            None,
            None,
            0,
            0,
            None,
            None,
            [None, None, 0, 1, 0],
            None,
            None,
            0,
            native[12],
            None,
            None,
        ]

    @staticmethod
    def _parse_added_source(
        remote: str,
        kind: str,
        name: str,
        mime_type: str,
        result: Any,
        existing_resources: Any,
    ):
        existing = {resource.id for resource in existing_resources}
        matches: dict[str, Any] = {}
        for part in result:
            if not isinstance(part, list):
                continue
            for row in part:
                if field(row, 0) != remote:
                    continue
                bundle = field(field(row, 1), 10)
                sources = field(bundle, 0)
                if not isinstance(sources, list):
                    continue
                for source in sources:
                    source_id = field(source, 5)
                    if (
                        isinstance(source_id, str)
                        and source_id.startswith(remote + "/sources/")
                        and source_id not in existing
                        and field(source, 2) == name
                    ):
                        matches[source_id] = source
        if len(matches) == 1:
            source_id, source = matches.popitem()
            return Attachment(
                source_id,
                name,
                mime_type=mime_type,
                metadata={"kind": kind, "native": source, "source_type": field(source, 1)},
            )
        if kind == "file":
            raise WebAIProviderError("Gemini did not confirm the added file; sync before retrying")
        return result


class BaseGeminiWebAIProvider(BaseWebAIProvider):
    """Provider identity and protocol rules shared by both execution modes."""

    provider_type = WebAIProviderType.GEMINI
    stream_idle_timeout = 300
    stream_total_timeout = 600

    _capabilities = frozenset(
        {
            Capability.CHAT,
            Capability.HISTORY,
            Capability.DELETE,
            Capability.AUTH,
            Capability.MODELS,
            Capability.EFFORT,
            Capability.ATTACHMENTS,
            Capability.TEMPORARY,
            Capability.MESSAGE_EDIT,
            Capability.MESSAGE_RETRY,
            Capability.CHAT_BRANCH,
            Capability.RENAME,
            Capability.PROJECTS,
            Capability.PROJECT_CREATE,
            Capability.PROJECT_UPDATE,
            Capability.PROJECT_DELETE,
            Capability.RESOURCES,
        }
    )

    @staticmethod
    def _usable_chat_metadata(metadata: Any, conversation_id: str | None) -> bool:
        return (
            isinstance(metadata, (list, tuple))
            and len(metadata) >= 3
            and metadata[0] == conversation_id
            and all(isinstance(metadata[index], str) and metadata[index] for index in (1, 2))
        )

    @staticmethod
    def _extract_history_metadata(part0: Any, conversation_id: str) -> list[Any]:
        turns = rpc_value(part0, 0, default=[])
        turn = turns[0] if turns else None
        return [
            conversation_id,
            rpc_value(turn, 0, 1) or "",
            rpc_value(turn, 3, 0, 0, 0) or "",
            None,
            None,
            None,
            None,
            None,
            None,
            "",
        ]

    @staticmethod
    def _apply_continuation_parts(request: Any, metadata: list[Any], parts: list[Any]):
        turns = rpc_value(parts[0], 0, default=[])
        turn = turns[0] if turns else None
        action_files = None
        if request.action:
            if request.message_id is None:
                raise ValueError("message action requires a message ID")
            role = "user" if request.action == MessageAction.EDIT else "assistant"
            selected_index = action_turn_index(
                parts[0], request.message_id, role, request.action.value
            )
            original = turns[selected_index]
            action_files = rpc_value(original, 2, 0, 3) or []
            if not isinstance(action_files, list):
                raise WebAIProviderError("Gemini message attachments cannot be preserved")
            if request.action == MessageAction.RETRY:
                request = replace(
                    request,
                    prompt=rpc_value(original, 2, 0, 0) or "",
                    action=MessageAction.EDIT if selected_index > 0 else MessageAction.RETRY,
                )
            turn = turns[selected_index + 1] if selected_index + 1 < len(turns) else None
        rid = rpc_value(turn, 0, 1)
        rcid = rpc_value(turn, 3, 0, 0, 0)
        if turn is not None and (not rid or not rcid):
            raise GeminiCompatibilityError(
                "Could not retrieve Gemini conversation continuation IDs."
            )
        metadata[1:3] = [rid or "", rcid or ""]
        return request, metadata, action_files

    @staticmethod
    def _finalize_download_source(
        target: dict[str, Any] | None,
        url: str,
        cookie_values: dict[str, Any] | None,
    ) -> DownloadSource:
        if target is None and valid_media_url(url):
            target = {"url": url, "name": "image"}
        if not target or not valid_media_url(target["url"]):
            raise ValueError("The provider did not return the file URL; sync the conversation")
        headers = {"Referer": "https://gemini.google.com/"}
        host = urlsplit(target["url"]).hostname
        if (
            host in {"lh3.googleusercontent.com", "lh3-gg.googleusercontent.com"}
            and cookie_values is not None
        ):
            headers["Cookie"] = cookie_header(cookie_values, ("__Secure-1PSID", "__Secure-1PSIDTS"))
        return DownloadSource(target["url"], target["name"], True, headers, NativeDownloadClient)

    def _active_transport(self) -> Any:
        return self._connected_transport

    def _invalidate_project_cache(self, project_id: str) -> None:
        self._project_contexts.remove(notebook_id(project_id))

    def _take_chat_metadata(self, conversation_id: str):
        return self._chat_metadata.get(conversation_id)

    def _remember_chat_metadata(self, conversation_id: str, metadata: Any) -> None:
        self._chat_metadata.put(conversation_id, list(metadata))

    def _session_status(self) -> SessionStatus:
        saved = session_exists(self.cookie_file)
        if self._client:
            state = SessionState.AUTHENTICATED
        elif saved:
            state = SessionState.UNKNOWN
        else:
            state = SessionState.LOGIN_REQUIRED
        return SessionStatus(state, saved)

    def _remember_response(self, cid: str, metadata: Any, *, temporary: bool) -> None:
        if cid and not temporary and self._usable_chat_metadata(metadata, cid):
            self._remember_chat_metadata(cid, metadata)
        if temporary:
            self._temporary[cid] = metadata
            while len(self._temporary) > 32:
                self._temporary.pop(next(iter(self._temporary)))

    @staticmethod
    def _parse_list_chats_args(cursor: str | None, limit: int) -> int:
        try:
            offset = int(cursor or 0)
        except (ValueError, TypeError):
            raise ValueError("invalid Gemini conversation cursor") from None
        if offset < 0 or not 1 <= limit <= 100:
            raise ValueError("invalid Gemini conversation page")
        return offset

    def _build_chat_from_history(self, conversation_id: str, parts: list[Any]) -> Conversation:
        if len(parts) != 1:
            raise GeminiCompatibilityError("Gemini history response format changed")
        chat = parse_chat_history(parts[0], conversation_id)
        metadata = self._extract_history_metadata(parts[0], conversation_id)
        if conversation_id not in self._temporary and self._usable_chat_metadata(
            metadata, conversation_id
        ):
            self._remember_chat_metadata(conversation_id, metadata)
        return chat

    @staticmethod
    def _parse_branch_result(result: Any, conversation_id: str) -> tuple[str, Any]:
        remote = rpc_value(result, 0, 0, 0)
        if not isinstance(remote, str) or not remote.startswith("c_") or remote == conversation_id:
            raise WebAIProviderError("Gemini did not confirm a new conversation")
        return remote, rpc_value(result, 0, 0, 1)

    @staticmethod
    def _apply_branch_title(chat: Conversation, title: Any) -> Conversation:
        if isinstance(title, str) and title.strip():
            return replace(
                chat, title=title, metadata={**chat.metadata, "title_unavailable": False}
            )
        return chat

    @staticmethod
    def _validate_chat_title(title: Any) -> str:
        if not isinstance(title, str) or not 1 <= len(title.strip()) <= 100:
            raise ValueError("Gemini chat title must contain 1–100 characters")
        return title.strip()

    @staticmethod
    def _validate_create_project(name: str, instructions: str) -> str:
        if not name.strip() or instructions:
            raise ValueError(
                "Provide only a name when creating a notebook; edit instructions afterward"
            )
        return name.strip()

    def _cache_notebook_context(self, project: str, notebook: ProjectData) -> tuple[str, list[Any]]:
        value = field(field(notebook.metadata.get("native"), 1), 12)
        if not isinstance(value, list) or not value or value[0] not in (1, 2):
            raise WebAIProviderError("Gemini notebook context could not be read")
        self._project_contexts.put(project, list(value))
        return project, value

    def _prepare_continuation_lookup(
        self, request: Any, timing: Any
    ) -> tuple[bool, list[Any], Any, bool]:
        metadata = self._temporary.get(request.conversation_id) if request.temporary else None
        if request.temporary and request.conversation_id and metadata is None:
            raise WebAIProviderError("Temporary Gemini context is no longer in memory")
        if metadata is not None:
            return True, metadata, None, False

        cached = (
            self._take_chat_metadata(request.conversation_id) if request.conversation_id else None
        )
        usable = self._usable_chat_metadata(cached, request.conversation_id)
        if cached is not None and usable and not request.action:
            if timing is not None:
                timing.increment("pre_send_conversation_cache_hit")
            return True, list(cached), None, False

        if timing is not None and request.conversation_id:
            timing.increment("pre_send_conversation_cache_miss")
        fresh = [request.conversation_id or "", "", "", None, None, None, None, None, None, ""]
        if not request.conversation_id:
            return True, fresh, None, False

        suffix = "user" if request.action == MessageAction.EDIT else "model"
        recent_action = bool(
            request.action and usable and request.message_id == f"{cached[1]}:{suffix}"
        )
        return False, fresh, cached[1] if recent_action else None, recent_action

    @staticmethod
    def _needs_full_continuation_history(parts: Any, expected_turn_id: Any) -> bool:
        turns = rpc_value(parts, 0, 0, default=[])
        return (
            not isinstance(turns, list)
            or len(turns) != 2
            or rpc_value(turns[0], 0, 1) != expected_turn_id
            or not rpc_value(turns[1], 0, 1)
            or not rpc_value(turns[1], 3, 0, 0, 0)
        )

    @staticmethod
    def _is_stable_media_reference(url: str) -> tuple[bool, bool]:
        stable = bool(PLACEHOLDER.fullmatch(url) or url.startswith("gemini-media:"))
        return stable, stable or valid_media_url(url)

    @staticmethod
    def _extract_media_target_from_chat(chat: Conversation, url: str) -> dict[str, Any] | None:
        target = None
        for message in chat.messages:
            for content in message.content:
                resource = content.resource
                if resource and resource.id == url:
                    target = {"url": resource.remote_url, "name": resource.name}
        return target

    @staticmethod
    def _download_needs_cookies(target: dict[str, Any] | None, url: str) -> bool:
        host = urlsplit(target["url"] if target else url).hostname
        return host in {"lh3.googleusercontent.com", "lh3-gg.googleusercontent.com"}

    @staticmethod
    def _deferred_upload_record(
        filename: str, mime: str, size: int, local_path: str | None
    ) -> dict[str, Any]:
        return {
            "id": filename,
            "path": local_path,
            "name": filename,
            "mime_type": mime,
            "size_bytes": size,
            "deferred_upload": True,
        }
