"""Live checks for the authenticated account's available model catalog."""

import os
import re
from contextlib import suppress

from webaiprovider.base import Capability
from webaiprovider.errors import RemoteNotFound

from .support.case import LiveAsyncTestCase, LiveSyncTestCase

_CLAUDE_5_MODEL_RE = re.compile(r"^claude-([a-z]+)-5(?:-(\d+))?(?:-\d{8})?$")


def _select_live_models(provider: str, models):
    """Narrow live model execution to the current generation unless WEBAI_LIVE_ALL_MODELS=1."""
    if os.environ.get("WEBAI_LIVE_ALL_MODELS") == "1":
        return models
    if provider == "chatgpt":
        preferred = [item for item in models if item.id in {"gpt-6-instant", "gpt-6-thinking"}]
        if preferred:
            return preferred
        gpt6 = [item for item in models if item.id == "gpt-6" or item.id.startswith("gpt-6-")]
        return gpt6 or models
    if provider == "claude":
        max_minor_by_family: dict[str, int] = {}
        parsed: list[tuple[str, int] | None] = []
        for item in models:
            match = _CLAUDE_5_MODEL_RE.match(item.id)
            if not match:
                parsed.append(None)
                continue
            family, minor = match.group(1), int(match.group(2) or 0)
            parsed.append((family, minor))
            if minor > max_minor_by_family.get(family, -1):
                max_minor_by_family[family] = minor
        latest_claude_5 = [
            item
            for item, info in zip(models, parsed, strict=True)
            if info is not None and info[1] == max_minor_by_family[info[0]]
        ]
        return latest_claude_5 or models
    if provider == "gemini":
        preferred = [item for item in models if item.id in {"gemini-hydrogen", "gemini-neon"}]
        return preferred or models
    return models


def _catalog_and_model_steps(case):
    capabilities = case.context.session.capabilities
    if Capability.MODELS not in capabilities:
        case.skipTest("Provider does not expose model catalogs")

    catalog = case.context.session.models
    models = yield case.call_api("models.refresh", catalog.refresh)
    case.assertTrue(models, "Authenticated account returned an empty model catalog")
    model = models[0]
    case.assertEqual((yield catalog.get(model.id)), model)
    case.assertEqual((yield catalog.find(model.id)), model)
    case.assertIn(model, (yield catalog.search(model.id, exact=True)))
    resolved_model = yield catalog.resolve(model)
    case.assertEqual(resolved_model.model, model)
    if efforts := model.efforts.list():
        resolved_effort = yield catalog.resolve(efforts[0])
        case.assertEqual(resolved_effort.effort, efforts[0])

    use_temporary = Capability.TEMPORARY in capabilities
    if Capability.CHAT not in capabilities or (
        not use_temporary and Capability.DELETE not in capabilities
    ):
        case.context.skip("models.send: unavailable_or_cannot_safely_clean_up")
        return

    models = _select_live_models(case.context.account.provider, models)

    for model in models:
        marker = f"{case.context.marker}-m-{model.id}"
        entry = None if use_temporary else case.context.new_remote("chat", marker)
        chat = None
        try:
            chat = yield case.context.session.chats.create(title=marker, temporary=use_temporary)
            if entry is not None:
                case.context.record_chat_id(entry, chat)
            efforts = model.efforts.list()
            selected_effort = (
                next((item for item in efforts if item.id == "standard"), efforts[0])
                if efforts
                else None
            )
            op = chat.responses.create(
                "Reply with only: OK" if entry is None else f"{marker} Reply with only: OK",
                model=selected_effort if selected_effort is not None else model,
            )
            response = yield case.call_or_skip_rate_limit(
                f"models.send[{model.id}]", op.start, entry=entry, chat=chat
            )
            if entry is not None:
                case.context.record_chat_id(entry, chat)
            case.assertTrue(
                getattr(response, "text", "").strip(),
                f"Model {model.id} returned empty text",
            )
        finally:
            if entry is not None:
                yield case.cleanup_entry(entry)
            elif chat is not None and chat.remote_id:
                with suppress(RemoteNotFound):
                    yield case.call_api(
                        "models.delete_temporary",
                        chat.delete,
                        expected_errors=(RemoteNotFound,),
                    )


class AsyncModelApiTests(LiveAsyncTestCase):
    async def test_catalog_and_model_execution(self) -> None:
        await self.run_steps(_catalog_and_model_steps(self))


class SyncModelApiTests(LiveSyncTestCase):
    def test_catalog_and_model_execution(self) -> None:
        self.run_steps(_catalog_and_model_steps(self))
