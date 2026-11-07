from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, AsyncIterator

from .base import SendRequest
from .catalogs.policies import Efforts, Models
from .catalogs.resolver import SelectionResolver
from .contracts import (
    Artifact,
    ArtifactDiscovered,
    ModelRef,
    OutputBegin,
    OutputDelta,
    OutputEnd,
    OutputReplace,
    Source,
    SourceDiscovered,
    StreamEvent,
    ThinkingBegin,
    ThinkingDelta,
    ThinkingEnd,
)
from .errors import ProviderError, UnsupportedOperationError

if TYPE_CHECKING:
    from .accounts.account import Account
    from .resources.chat import Chat
    from .resources.operation import MessageOperation
    from .resources.project import Project
    from .session import ProviderSession


class ProviderBridge:
    def __init__(self, account: Account) -> None:
        self.account = account
        self._provider_instance: Any = None

    def _get_provider(self) -> Any:
        if self._provider_instance is not None:
            return self._provider_instance

        provider_name = self.account.provider.lower()
        store = self.account.store()

        if provider_name == "chatgpt":
            from .providers.chatgpt.adapter import ChatGPTProvider
            from .providers.chatgpt.session import BrowserSession, SessionPaths

            paths = SessionPaths(store)
            session = BrowserSession(paths)
            self._provider_instance = ChatGPTProvider(session, account_id=self.account.id)
        elif provider_name == "gemini":
            from .providers.gemini.adapter import GeminiProvider

            self._provider_instance = GeminiProvider(store, account_id=self.account.id)
        elif provider_name == "claude":
            from .providers.claude.adapter import ClaudeProvider

            self._provider_instance = ClaudeProvider(store, account_id=self.account.id)
        elif provider_name == "grok":
            from .providers.grok.adapter import GrokProvider

            self._provider_instance = GrokProvider(store, account_id=self.account.id)
        else:
            raise UnsupportedOperationError(f"Desteklenmeyen sağlayıcı: {provider_name}")

        return self._provider_instance

    async def list_models(self, session: ProviderSession) -> list[ModelRef]:
        provider = self._get_provider()
        catalog = await provider.models()
        result: list[ModelRef] = []
        for m in getattr(catalog, "models", ()):
            efforts = tuple(e.id for e in getattr(m, "efforts", ()))
            result.append(
                ModelRef(
                    provider=self.account.provider,
                    id=m.id,
                    name=getattr(m, "title", m.id),
                    capabilities=efforts,
                    metadata=getattr(m, "metadata", {}),
                )
            )
        return result

    async def list_chats(self, session: ProviderSession) -> list[Chat]:
        provider = self._get_provider()
        if not hasattr(provider, "list_chats"):
            return []

        from .resources.chat import Chat

        chats_data = await provider.list_chats()
        items = (
            getattr(chats_data, "items", chats_data)
            if not isinstance(chats_data, list)
            else chats_data
        )
        result: list[Chat] = []
        for item in items:
            remote_id = getattr(item, "remote_id", None) or getattr(item, "id", None)
            title = getattr(item, "title", "Adsız Sohbet")
            project_id = getattr(item, "project_id", None)
            result.append(
                Chat(
                    title=title,
                    remote_id=str(remote_id) if remote_id else None,
                    project_id=str(project_id) if project_id else None,
                    session=session,
                )
            )
        return result

    async def create_chat(self, chat: Chat, **options: Any) -> None:
        pass

    async def rename_chat(self, chat: Chat, title: str) -> None:
        if not chat.remote_id:
            return
        provider = self._get_provider()
        if hasattr(provider, "rename_chat"):
            await provider.rename_chat(chat.remote_id, title)

    async def delete_chat(self, chat: Chat) -> None:
        if not chat.remote_id:
            return
        provider = self._get_provider()
        if hasattr(provider, "delete_chat"):
            await provider.delete_chat(chat.remote_id)

    async def branch_chat(self, chat: Chat, message_id: str, *, title: str | None = None) -> Chat:
        if not chat.remote_id:
            raise UnsupportedOperationError("Sohbet uzakta kaydedilmediğinden dallandırılamaz")
        provider = self._get_provider()
        if not hasattr(provider, "branch_chat"):
            raise UnsupportedOperationError(
                f"{self.account.provider} sohbet dallandırmayı desteklemiyor"
            )

        from .resources.chat import Chat

        res = await provider.branch_chat(chat.remote_id, message_id)
        new_remote_id = getattr(res, "remote_id", None) or getattr(res, "id", None) or str(res)
        return Chat(
            title=title or f"{chat.title} (Branch)",
            remote_id=new_remote_id,
            project_id=chat.project_id,
            session=chat.session,
        )

    async def move_chat(self, chat: Chat, project_id: str | None) -> None:
        if not chat.remote_id:
            return
        provider = self._get_provider()
        if hasattr(provider, "move_chat"):
            await provider.move_chat(chat.remote_id, project_id)

    async def list_projects(self, session: ProviderSession) -> list[Project]:
        provider = self._get_provider()
        if not hasattr(provider, "list_projects"):
            return []

        from .resources.project import Project

        data = await provider.list_projects()
        items = getattr(data, "items", data) if not isinstance(data, list) else data
        result: list[Project] = []
        for item in items:
            remote_id = getattr(item, "remote_id", None) or getattr(item, "id", None)
            title = getattr(item, "title", "Adsız Proje")
            result.append(
                Project(
                    title=title,
                    remote_id=str(remote_id) if remote_id else None,
                    session=session,
                )
            )
        return result

    async def create_project(
        self, project: Project, instruction: str | None = None, **options: Any
    ) -> None:
        provider = self._get_provider()
        if hasattr(provider, "create_project"):
            res = await provider.create_project(project.title, instruction=instruction, **options)
            project.remote_id = getattr(res, "remote_id", None) or getattr(res, "id", None)

    async def rename_project(self, project: Project, title: str) -> None:
        if not project.remote_id:
            return
        provider = self._get_provider()
        if hasattr(provider, "rename_project"):
            await provider.rename_project(project.remote_id, title)

    async def delete_project(self, project: Project) -> None:
        if not project.remote_id:
            return
        provider = self._get_provider()
        if hasattr(provider, "delete_project"):
            await provider.delete_project(project.remote_id)

    async def update_project_instruction(self, project: Project, instruction: str) -> None:
        if not project.remote_id:
            return
        provider = self._get_provider()
        if hasattr(provider, "update_project_instruction"):
            await provider.update_project_instruction(project.remote_id, instruction)

    async def add_project_resource(self, project: Project, item: dict[str, Any]) -> None:
        if not project.remote_id:
            return
        provider = self._get_provider()
        if hasattr(provider, "add_project_resource"):
            await provider.add_project_resource(project.remote_id, item)

    async def remove_project_resource(self, project: Project, resource_id: str) -> None:
        if not project.remote_id:
            return
        provider = self._get_provider()
        if hasattr(provider, "remove_project_resource"):
            await provider.remove_project_resource(project.remote_id, resource_id)

    def _get_static_catalog(self) -> Any:
        p = self.account.provider.lower()
        if p == "chatgpt":
            from .catalogs.chatgpt import ChatGPTModels

            return ChatGPTModels
        if p == "claude":
            from .catalogs.claude import ClaudeModels

            return ClaudeModels
        if p == "gemini":
            from .catalogs.gemini import GeminiModels

            return GeminiModels
        if p == "grok":
            from .catalogs.grok import GrokModels

            return GrokModels
        return None

    async def execute_stream(self, operation: MessageOperation) -> AsyncIterator[StreamEvent]:
        provider = self._get_provider()
        msg = operation.message
        chat = operation.chat

        model_name: str | None = None
        if msg.model is not None:
            if isinstance(msg.model, ModelRef):
                model_name = msg.model.id
            elif isinstance(msg.model, Models):
                candidates = await self.list_models(chat.session) if chat.session else []
                catalog = self._get_static_catalog()
                if not candidates and catalog is not None:
                    candidates = catalog.list()
                if candidates:
                    resolved = SelectionResolver.resolve_model(msg.model, candidates)
                    model_name = resolved.id
                else:
                    model_name = msg.model.value
            elif isinstance(msg.model, str):
                catalog = self._get_static_catalog()
                if catalog is not None and catalog.contains(msg.model):
                    model_name = catalog.get(msg.model).id
                elif catalog is not None:
                    try:
                        resolved = SelectionResolver.resolve_model(msg.model, catalog.list())
                        model_name = resolved.id
                    except Exception:
                        model_name = msg.model
                else:
                    model_name = msg.model
            else:
                model_name = str(msg.model)

        effort_name: str | None = None
        if msg.effort is not None:
            if hasattr(msg.effort, "id"):
                effort_name = str(getattr(msg.effort, "id"))
            elif isinstance(msg.effort, Efforts):
                effort_name = msg.effort.value
            else:
                effort_name = str(msg.effort)

        attachment_records = []
        for att in msg.attachments:
            if att.path is not None:
                attachment_records.append(
                    {"path": str(att.path), "name": att.name, "mime": att.mime_type}
                )
            elif att._content is not None:
                attachment_records.append(
                    {"bytes": att._content, "name": att.name, "mime": att.mime_type}
                )

        req = SendRequest(
            provider_id=self.account.provider,
            account_id=self.account.id,
            chat_id=chat.id,
            prompt=msg.content,
            conversation_id=chat.remote_id,
            model=model_name,
            effort=effort_name,
            attachments=attachment_records,
            project_id=chat.project_id,
            action=msg.metadata.get("action"),
            message_id=msg.metadata.get("parent_message_id"),
        )

        output_started = False
        thinking_started = False

        try:
            async for chunk in provider.stream(req):
                chunk_type = type(chunk).__name__

                if hasattr(chunk, "conversation_id") and chunk.conversation_id:
                    chat.remote_id = str(chunk.conversation_id)

                if "Thought" in chunk_type:
                    if not thinking_started:
                        thinking_started = True
                        yield ThinkingBegin()

                    text = getattr(chunk, "text", "") or getattr(chunk, "delta", "")
                    if text:
                        yield ThinkingDelta(delta=text)
                    continue

                if thinking_started and not output_started:
                    thinking_started = False
                    yield ThinkingEnd()

                if "Replace" in chunk_type:
                    if not output_started:
                        output_started = True
                        yield OutputBegin()
                    text = getattr(chunk, "text", "")
                    yield OutputReplace(text=text)
                    continue

                delta = getattr(chunk, "delta", None) or getattr(chunk, "text", None)
                if delta:
                    if not output_started:
                        output_started = True
                        yield OutputBegin()
                    yield OutputDelta(delta=str(delta))

                if hasattr(chunk, "sources") and chunk.sources:
                    for s in chunk.sources:
                        yield SourceDiscovered(
                            Source(
                                id=str(getattr(s, "id", "")),
                                url=str(getattr(s, "url", "")),
                                title=str(getattr(s, "title", "")),
                                snippet=str(getattr(s, "snippet", "")),
                            )
                        )

                if hasattr(chunk, "files") and chunk.files:
                    for f in chunk.files:
                        yield ArtifactDiscovered(
                            Artifact(
                                id=str(getattr(f, "id", "")),
                                kind=str(getattr(f, "kind", "file")),
                                name=str(getattr(f, "name", "")),
                                download_url=str(getattr(f, "url", "")),
                            )
                        )

            if thinking_started:
                yield ThinkingEnd()

            if output_started:
                yield OutputEnd()

        except ProviderError:
            raise
        except Exception as exc:
            raise ProviderError(str(exc)) from exc

    async def close_session(self, session: ProviderSession) -> None:
        if self._provider_instance is not None:
            if hasattr(self._provider_instance, "close"):
                res = self._provider_instance.close()
                if asyncio.iscoroutine(res):
                    await res
            self._provider_instance = None


_BRIDGES: dict[tuple[str, str], ProviderBridge] = {}


def get_bridge(provider_name: str, account: Account) -> ProviderBridge:
    key = (provider_name.lower(), account.id)
    if key not in _BRIDGES:
        _BRIDGES[key] = ProviderBridge(account)
    return _BRIDGES[key]
