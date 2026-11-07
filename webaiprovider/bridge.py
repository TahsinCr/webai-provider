from __future__ import annotations

import asyncio
import os
from dataclasses import replace
from typing import TYPE_CHECKING, Any, AsyncIterator

from .base import (
    ActionBegin,
    ActionDelta,
    ActionEnd,
    ConversationBound,
    MessageAction,
    MessageMetadata,
    ProviderAdapter,
    ReplaceText,
    ResponseComplete,
    SendRequest,
    TextDelta,
    ThoughtDelta,
    validate_request,
)
from .contracts import (
    Artifact,
    ArtifactDiscovered,
    EffortRef,
    EffortSet,
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
from .models import (
    Conversation,
    ModelCatalog,
    Page,
)
from .models import (
    Project as ProviderProject,
)
from .models import (
    Resource as ProviderResource,
)
from .utils.files import DownloadSource, NativeDownloadClient

if TYPE_CHECKING:
    from .accounts.account import Account
    from .resources.chat import Chat
    from .resources.message import Message
    from .resources.operation import MessageOperation
    from .resources.project import Project
    from .session import Provider


def _resource_record(resource: ProviderResource) -> dict[str, Any]:
    return {
        "id": resource.id,
        "type": resource.metadata.get("kind", "file"),
        "name": resource.name,
        "media_type": resource.media_type,
        "url": resource.remote_url,
        "metadata": dict(resource.metadata),
    }


def _chat_from_conversation(item: Conversation, session: Provider) -> Chat:
    from .resources.chat import Chat

    return Chat(
        title=item.title,
        remote_id=item.origin.remote_id,
        project_id=item.project_id,
        session=session,
    )


class ProviderBridge:
    def __init__(self, account: Account) -> None:
        self.account = account
        self._provider_instance: ProviderAdapter | None = None
        self._models_cache: tuple[ModelRef, ...] | None = None

    async def open(self) -> None:
        provider = self._get_provider()
        await provider.prepare()

    def _get_provider(self) -> ProviderAdapter:
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
            raise UnsupportedOperationError(f"Unsupported provider: {provider_name}")

        return self._provider_instance

    async def list_models(self, *, refresh: bool = False) -> list[ModelRef]:
        if self._models_cache is not None and not refresh:
            return list(self._models_cache)
        provider = self._get_provider()
        catalog = await provider.models(force=refresh)
        result: list[ModelRef] = []
        if not isinstance(catalog, ModelCatalog):
            raise ProviderError("Provider returned an invalid model catalog")
        available_models = catalog.chat_models
        default_model = catalog.default_chat_model
        for rank, m in enumerate(available_models):
            efforts = tuple(
                EffortRef(
                    provider=self.account.provider,
                    model_id=m.id,
                    id=choice.id,
                    name=(
                        {"standard": "Medium", "extended": "High"}.get(choice.id.casefold())
                        if self.account.provider.casefold() == "chatgpt"
                        else None
                    )
                    or choice.title
                    or choice.id,
                    rank=index,
                )
                for index, choice in enumerate(m.efforts)
            )
            default_effort_id = m.default_effort
            if default_effort_id not in {effort.id for effort in efforts}:
                default_effort_id = None
            metadata = dict(m.metadata)
            if default_model is not None and default_model.id == m.id:
                metadata["default"] = True
            result.append(
                ModelRef(
                    provider=self.account.provider,
                    id=m.id,
                    name=m.title or m.id,
                    efforts=EffortSet.from_refs(
                        efforts,
                        name=f"{m.id}_Efforts",
                        default_id=default_effort_id,
                    ),
                    rank=rank,
                    metadata=metadata,
                )
            )
        self._models_cache = tuple(result)
        return list(self._models_cache)

    async def list_chats(self, session: Provider) -> list[Chat]:
        provider = self._get_provider()
        chats_data = await provider.list_chats()
        if not isinstance(chats_data, Page) or any(
            not isinstance(item, Conversation) for item in chats_data.items
        ):
            raise ProviderError("Provider returned an invalid conversation page")
        return [_chat_from_conversation(item, session) for item in chats_data.items]

    async def list_project_chats(self, project: Project, cursor: str | None = None) -> Page[Chat]:
        if project.session is None:
            raise ProviderError("Project is not attached to a provider session")
        if not project.remote_id:
            return Page(())
        page = await self._get_provider().list_project_chats(project.remote_id, cursor)
        if not isinstance(page, Page) or any(
            not isinstance(item, Conversation) for item in page.items
        ):
            raise ProviderError("Provider returned an invalid project conversation page")
        return Page(
            tuple(_chat_from_conversation(item, project.session) for item in page.items),
            page.next_cursor,
        )

    async def list_messages(self, chat: Chat) -> list[Message]:
        if not chat.remote_id:
            return list(chat.messages._items)
        from .resources.message import Message

        conversation = await self._get_provider().get_chat(chat.remote_id)
        if not isinstance(conversation, Conversation):
            raise ProviderError("Provider returned an invalid conversation")
        if conversation.title and not chat.title:
            chat.title = conversation.title
        messages: list[Message] = []
        for item in conversation.messages:
            if item.channel == "analysis":
                continue
            text = "\n".join(
                part.text for part in item.content if part.kind == "text" and part.text
            )
            messages.append(
                Message(
                    content=text,
                    role=item.role,
                    id=item.id,
                    metadata={**item.metadata, "actions": item.actions},
                    chat=chat,
                )
            )
        return messages

    async def rename_chat(self, chat: Chat, title: str) -> None:
        if chat.remote_id:
            await self._get_provider().rename_chat(chat.remote_id, title)

    async def delete_chat(self, chat: Chat) -> None:
        if chat.remote_id:
            await self._get_provider().delete_chat(chat.remote_id)

    async def branch_chat(self, chat: Chat, message_id: str, *, title: str | None = None) -> Chat:
        if not chat.remote_id:
            raise UnsupportedOperationError(
                "Cannot branch a conversation that has not been persisted remotely"
            )
        from .resources.chat import Chat

        branched = await self._get_provider().branch_chat(chat.remote_id, message_id)
        return Chat(
            title=title or branched.title,
            remote_id=branched.origin.remote_id,
            project_id=branched.project_id,
            session=chat.session,
        )

    async def move_chat(self, chat: Chat, project_id: str | None) -> None:
        if chat.remote_id:
            await self._get_provider().move_chat(chat.remote_id, project_id)

    async def list_projects(self, session: Provider) -> list[Project]:
        from .resources.project import Project

        result: list[Project] = []
        for native in (await self._get_provider().list_projects()).items:
            if not isinstance(native, ProviderProject):
                raise ProviderError("Provider returned an invalid project")
            project = Project(
                title=native.name,
                remote_id=native.origin.remote_id,
                session=session,
            )
            project.instruction._text = native.instructions or ""
            project.resources._items.extend(
                _resource_record(resource) for resource in native.resources
            )
            result.append(project)
        return result

    async def create_project(
        self,
        project: Project,
        instruction: str | None = None,
    ) -> None:
        created = await self._get_provider().create_project(
            project.title,
            instructions=instruction or "",
        )
        self._apply_created_project(project, created)

    async def create_chatgpt_project(
        self,
        project: Project,
        instruction: str | None = None,
        *,
        memory_share: bool = True,
    ) -> None:
        if self.account.provider != "chatgpt":
            raise UnsupportedOperationError("ChatGPT project settings require a ChatGPT provider")
        from .providers.chatgpt.adapter import ChatGPTProvider as ChatGPTAdapter

        adapter = self._get_provider()
        if not isinstance(adapter, ChatGPTAdapter):
            raise UnsupportedOperationError("ChatGPT project settings require a ChatGPT provider")
        created = await adapter.create_project(
            project.title,
            instructions=instruction or "",
            memory_share=memory_share,
        )
        self._apply_created_project(project, created)

    @staticmethod
    def _apply_created_project(project: Project, created: ProviderProject) -> None:
        if not isinstance(created, ProviderProject):
            raise ProviderError("Provider returned an invalid project")
        project.remote_id = created.origin.remote_id
        project.title = created.name
        project.instruction._text = created.instructions or ""
        project.resources._items.extend(
            _resource_record(resource) for resource in created.resources
        )

    async def update_project(
        self,
        project: Project,
        *,
        title: str | None = None,
        instructions: str | None = None,
    ) -> None:
        if not project.remote_id:
            raise UnsupportedOperationError("Project is not persisted by the provider")
        provider = self._get_provider()
        current = await provider.get_project(project.remote_id)
        updated = replace(
            current,
            name=title if title is not None else current.name,
            instructions=(instructions if instructions is not None else current.instructions),
        )
        saved = await provider.update_project(updated)
        project.title = saved.name
        project.instruction._text = saved.instructions or ""

    async def delete_project(self, project: Project) -> None:
        if project.remote_id:
            await self._get_provider().delete_project(project.remote_id)

    async def add_project_resource(self, project: Project, item: dict[str, Any]) -> dict[str, Any]:
        if not project.remote_id:
            return item

        kind = item["type"]
        data = item.get("data")
        options: dict[str, Any]
        if kind == "text" and isinstance(data, str):
            options = {
                "name": item.get("name") or item.get("title") or "Text",
                "text": data,
            }
        elif kind == "file":
            path = item.get("path") or data
            if not isinstance(path, (str, os.PathLike)):
                raise ValueError("File resources require a local path")
            options = {"path": os.fspath(path)}
        else:
            raise UnsupportedOperationError(f"Project resource type is unsupported: {kind}")

        known_ids = {record.get("id") for record in project.resources._items}
        updated = await self._get_provider().add_project_resource(
            project.remote_id, kind, **options
        )
        if not isinstance(updated, ProviderProject):
            raise ProviderError("Provider returned an invalid project")
        added = [resource for resource in updated.resources if resource.id not in known_ids]
        expected_name = options.get("name") or os.path.basename(options.get("path", ""))
        matching = [resource for resource in added if resource.name == expected_name]
        resource = matching[-1] if matching else added[0] if len(added) == 1 else None
        if resource is None:
            raise ProviderError("Provider did not confirm the created project resource")
        existing = {record.get("id"): record for record in project.resources._items}
        project.resources._items = [
            {
                **existing.get(remote.id, {}),
                **_resource_record(remote),
            }
            for remote in updated.resources
            if remote.id != resource.id
        ]
        return {
            **item,
            "id": resource.id,
            "name": resource.name,
            "media_type": resource.media_type,
            "url": resource.remote_url,
            "metadata": dict(resource.metadata),
        }

    async def remove_project_resource(self, project: Project, resource_id: str) -> None:
        if project.remote_id:
            await self._get_provider().remove_project_resource(project.remote_id, resource_id)

    async def execute_stream(self, operation: MessageOperation) -> AsyncIterator[StreamEvent]:
        provider = self._get_provider()
        message = operation.message
        chat = operation.chat

        model: str | None = None
        effort: str | None = None
        if message.model is not None:
            if chat.session is None:
                raise UnsupportedOperationError(
                    "Model selection requires an active provider session"
                )
            selection = await chat.session.models.resolve(message.model)
            model = selection.model.id
            effort = selection.effort.id if selection.effort is not None else None

        attachments = tuple(
            {
                key: value
                for key, value in {
                    "path": str(item.path) if item.path is not None else None,
                    "bytes": item._content,
                    "name": item.name,
                    "mime": item.mime_type,
                }.items()
                if value is not None
            }
            for item in message.attachments
        )
        raw_action = message.metadata.get("action")
        try:
            action = MessageAction(raw_action) if raw_action is not None else None
        except ValueError:
            raise UnsupportedOperationError("Unsupported message action") from None

        request = SendRequest(
            provider_id=self.account.provider,
            account_id=self.account.id,
            chat_id=chat.id,
            prompt=message.content,
            conversation_id=chat.remote_id,
            model=model,
            effort=effort,
            project_id=chat.project_id,
            temporary=bool(message.metadata.get("temporary", False)),
            attachments=attachments,
            action=action,
            message_id=message.metadata.get("parent_message_id"),
        )
        validate_request(provider, request)

        output_started = False
        thinking_started = False
        thinking_text = ""

        try:
            async for chunk in provider.stream(request):
                if isinstance(chunk, ConversationBound):
                    chat.remote_id = chunk.conversation_id
                    continue

                if isinstance(chunk, ThoughtDelta) or (
                    isinstance(chunk, (TextDelta, ReplaceText)) and chunk.channel == "analysis"
                ):
                    if isinstance(chunk, ReplaceText):
                        delta = (
                            chunk.text[len(thinking_text) :]
                            if chunk.text.startswith(thinking_text)
                            else chunk.text
                        )
                        thinking_text = chunk.text
                    else:
                        delta = chunk.text
                        thinking_text += delta
                    if not thinking_started:
                        thinking_started = True
                        yield ThinkingBegin()
                    if delta:
                        yield ThinkingDelta(delta=delta)
                    continue

                if isinstance(chunk, ReplaceText):
                    if not chunk.text and not output_started:
                        continue
                    if thinking_started:
                        thinking_started = False
                        yield ThinkingEnd(text=thinking_text)
                    if not output_started:
                        output_started = True
                        yield OutputBegin()
                    yield OutputReplace(text=chunk.text)
                    continue

                if isinstance(chunk, TextDelta):
                    if chunk.text:
                        if thinking_started:
                            thinking_started = False
                            yield ThinkingEnd(text=thinking_text)
                        if not output_started:
                            output_started = True
                            yield OutputBegin()
                        yield OutputDelta(delta=chunk.text)
                    continue

                if isinstance(chunk, MessageMetadata):
                    if chunk.user_message_id:
                        operation.message.id = chunk.user_message_id
                    if chunk.message_id:
                        operation._assistant_message_id = chunk.message_id
                    for record in chunk.sources:
                        yield SourceDiscovered(
                            Source(
                                id=str(record.get("id") or record.get("source_id") or ""),
                                url=str(record.get("url") or ""),
                                title=str(record.get("title") or ""),
                                snippet=str(record.get("snippet") or ""),
                                kind=str(record.get("kind") or "web"),
                                metadata=record,
                            )
                        )
                    for record in chunk.files:
                        size = record.get("size")
                        yield ArtifactDiscovered(
                            Artifact(
                                id=str(record.get("id") or record.get("file_id") or ""),
                                kind=str(record.get("kind") or "file"),
                                name=str(
                                    record.get("name")
                                    or record.get("filename")
                                    or record.get("file_name")
                                    or ""
                                ),
                                mime_type=str(
                                    record.get("mime_type") or "application/octet-stream"
                                ),
                                size=size if isinstance(size, int) else None,
                                download_url=record.get("download_url") or record.get("url"),
                                content=record.get("content"),
                                metadata=record,
                            )
                        )
                    continue

                if isinstance(chunk, (ActionBegin, ActionDelta, ActionEnd, ResponseComplete)):
                    continue

            if thinking_started:
                yield ThinkingEnd(text=thinking_text)
            if output_started:
                yield OutputEnd()

        except ProviderError:
            raise
        except Exception as exc:
            raise ProviderError("Provider request failed") from exc

    async def download(
        self,
        url_or_artifact: Artifact | str,
        *,
        conversation_id: str | None = None,
        message_id: str | None = None,
    ) -> bytes:
        url = (
            url_or_artifact.download_url
            if isinstance(url_or_artifact, Artifact)
            else url_or_artifact
        )
        if not url:
            raise ValueError("Artifact has no download URL")

        source = await self._get_provider().resolve_download(
            url,
            conversation_id=conversation_id,
            message_id=message_id,
        )
        if not isinstance(source, DownloadSource):
            raise UnsupportedOperationError(
                "Provider did not resolve this URL to a safe download source"
            )

        def read_source() -> bytes:
            factory = source.client_factory
            client = (
                NativeDownloadClient(allow_private=False)
                if factory is None or factory is NativeDownloadClient
                else factory()
            )
            if not isinstance(client, NativeDownloadClient):
                raise ProviderError("Provider returned an unsupported download transport")
            with client:
                with client.stream("GET", source.url, headers=dict(source.headers)) as response:
                    if not 200 <= response.status_code < 300:
                        raise ProviderError(
                            f"File transfer failed with HTTP {response.status_code}"
                        )
                    return b"".join(response.iter_bytes())

        try:
            return await asyncio.to_thread(read_source)
        except ProviderError:
            raise
        except Exception as exc:
            raise ProviderError("File transfer failed") from exc

    async def close(self) -> None:
        if self._provider_instance is not None:
            if hasattr(self._provider_instance, "close"):
                res = self._provider_instance.close()
                if asyncio.iscoroutine(res):
                    await res
            self._provider_instance = None
