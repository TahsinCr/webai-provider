"""Origin-scoped Grok web transport and workspace editor."""

import asyncio
import base64
import json
import mimetypes
from urllib.parse import quote

from ..errors import AccountPaused, ProviderError
from ..projects import ProjectOptions, source_file
from ..session import WebSession

ORIGIN = "https://grok.com"
_MAX_LINE_BYTES = 1_048_576
_MAX_STREAM_BYTES = 64 * 1024 * 1024
_MAX_WORKSPACE_FILE_BYTES = 3 * 1024 * 1024


def remote_id(value):
    if not isinstance(value, str) or not value or len(value) > 512:
        raise ValueError("Grok kayıt kimliği geçersiz")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("Grok kayıt kimliği geçersiz")
    return quote(value, safe="")


def _authenticated(value):
    if not isinstance(value, dict):
        return False
    for key in ("isAuthenticated", "authenticated", "isLoggedIn"):
        if key in value:
            return value[key] is True
    user = value.get("user") or value.get("userInfo") or value.get("account")
    if isinstance(user, dict):
        return any(user.get(key) for key in ("id", "userId", "email", "username"))
    return any(value.get(key) for key in ("userId", "accountId", "email", "username"))


def _read_limited(path):
    with path.open("rb") as stream:
        return stream.read(_MAX_WORKSPACE_FILE_BYTES + 1)


class GrokTransport:
    def __init__(self, credential_file, *, client_factory=None):
        self.session = WebSession(ORIGIN, credential_file, None, client_factory=client_factory)
        self._auth_lock = asyncio.Lock()

    async def authenticate(self):
        async with self._auth_lock:
            return await self._check_authentication()

    async def _ensure_authenticated(self):
        if not self.session.authenticated:
            async with self._auth_lock:
                if not self.session.authenticated:
                    await self._check_authentication()

    async def _check_authentication(self):
        try:
            status = await self.session.request("GET", "/api/auth-status")
        except AccountPaused:
            self.session.authenticated = False
            raise
        if not _authenticated(status):
            self.session.authenticated = False
            raise AccountPaused(
                "Grok oturumu doğrulanamadı; Hesaplar bölümünden yeniden doğrulayın"
            )
        self.session.authenticated = True
        return status

    async def request(self, method, path, **options):
        await self._ensure_authenticated()
        try:
            return await self.session.request(method, path, **options)
        except AccountPaused:
            self.session.authenticated = False
            raise

    async def events(self, path, body):
        await self._ensure_authenticated()
        client = self.session.connect()
        total = 0
        try:
            async with client.stream(
                "POST",
                self.session.url(path),
                json=body,
                headers={"accept": "application/x-ndjson, text/event-stream"},
                allow_redirects=False,
                timeout=600,
            ) as response:
                self.session.check(response)
                async for line in response.aiter_lines():
                    if isinstance(line, bytes):
                        size = len(line)
                        total += size + 1
                        if size > _MAX_LINE_BYTES or total > _MAX_STREAM_BYTES:
                            raise ProviderError("Grok akış yanıtı boyut sınırını aştı")
                        line = line.decode("utf-8")
                    else:
                        size = len(line.encode("utf-8"))
                        total += size + 1
                        if size > _MAX_LINE_BYTES or total > _MAX_STREAM_BYTES:
                            raise ProviderError("Grok akış yanıtı boyut sınırını aştı")
                    line = line.strip()
                    if not line or line == "[DONE]":
                        continue
                    if line.startswith("data:"):
                        line = line[5:].lstrip()
                        if not line or line == "[DONE]":
                            continue
                    try:
                        event = json.loads(line)
                    except ValueError:
                        raise ProviderError("Grok akış yanıtının biçimi değişmiş") from None
                    if not isinstance(event, dict):
                        raise ProviderError("Grok akış yanıtının biçimi değişmiş")
                    yield event
        finally:
            await self.session.persist()

    async def close(self):
        await self.session.close()
        self.session.authenticated = False


class GrokProjectEditor:
    def __init__(self, transport):
        self.transport = transport

    async def options(self, project_id=None):
        return ProjectOptions(
            fields=("name", "instructions") if project_id else ("name",),
            sources=(
                {"id": "text", "title": "Metin"},
                {"id": "file", "title": "Dosya", "accept": "*/*"},
            )
            if project_id
            else (),
            remove_sources=bool(project_id),
        )

    async def write(self, operation, project_id=None, **values):
        project_path = "/rest/workspaces/" + remote_id(project_id)
        if operation == "update":
            current = await self.transport.request("GET", project_path)
            if not isinstance(current, dict):
                raise ProviderError("Grok proje yanıtının biçimi değişmiş")
            body = {
                "name": values.get("name", current.get("name", "")),
                "icon": current.get("icon"),
                "customPersonality": values.get(
                    "instructions", current.get("customPersonality", "")
                ),
                "preferredModel": current.get("preferredModel"),
                "conversationStarters": current.get("conversationStarters"),
                "isPublic": current.get("isPublic"),
            }
            return await self.transport.request("PUT", project_path, json=body)
        if operation == "add_source":
            if values.get("kind") not in ("text", "file"):
                raise ValueError("Grok yalnızca metin ve dosya kaynaklarını destekler")
            if values["kind"] == "text":
                name, content = values.get("name"), values.get("text")
                if not isinstance(name, str) or not name.strip() or not isinstance(content, str):
                    raise ValueError("Kaynak adı ve metin içeriği gerekir")
                mime_type = "text/plain"
                content = content.encode("utf-8")
            else:
                file = source_file(
                    values["path"], error="Grok çalışma alanı dosyaları en fazla 3 MiB olabilir"
                )
                if file.stat().st_size > _MAX_WORKSPACE_FILE_BYTES:
                    raise ValueError("Grok çalışma alanı dosyaları en fazla 3 MiB olabilir")
                content = await asyncio.to_thread(_read_limited, file)
                name = file.name
                mime_type = mimetypes.guess_type(file.name)[0] or "text/plain"
            if len(content) > _MAX_WORKSPACE_FILE_BYTES:
                raise ValueError("Grok çalışma alanı dosyaları en fazla 3 MiB olabilir")
            return await self.transport.request(
                "POST",
                project_path + "/files/upload",
                json={
                    "fileName": name.strip(),
                    "mimeType": mime_type,
                    "content": base64.b64encode(content).decode("ascii"),
                },
            )
        if operation == "remove_source":
            source = await self.transport.request(
                "GET", project_path + "/files", params={"recursive": "true"}
            )
            files = source.get("files", []) if isinstance(source, dict) else []
            item = next(
                (
                    entry
                    for entry in files
                    if isinstance(entry, dict)
                    and (entry.get("assetId") or entry.get("path")) == values["resource_id"]
                ),
                None,
            )
            if item is None or item.get("isDirectory"):
                raise ValueError("Grok proje kaynağı bulunamadı")
            if item.get("assetId"):
                return await self.transport.request(
                    "DELETE", project_path + "/assets/" + remote_id(item["assetId"])
                )
            return await self.transport.request(
                "DELETE", project_path + "/files", params={"path": item["path"]}
            )
        raise ValueError("Grok proje işlemi desteklenmiyor")
