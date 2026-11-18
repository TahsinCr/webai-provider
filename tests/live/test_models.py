"""Live checks for the authenticated account's available model catalog."""

import os
from contextlib import suppress

from webaiprovider.base import Capability
from webaiprovider.errors import RemoteNotFound

from .support.case import LiveAsyncTestCase


class AsyncModelApiTests(LiveAsyncTestCase):
    async def test_catalog_and_model_execution(self) -> None:
        capabilities = self.context.session.capabilities
        if Capability.MODELS not in capabilities:
            self.skipTest("Provider does not expose model catalogs")

        catalog = self.context.session.models
        models = await self.call_api("models.refresh", catalog.refresh)
        self.assertTrue(models, "Authenticated account returned an empty model catalog")
        model = models[0]
        self.assertEqual(await catalog.get(model.id), model)
        self.assertEqual(await catalog.find(model.id), model)
        self.assertIn(model, await catalog.search(model.id, exact=True))
        self.assertEqual((await catalog.resolve(model)).model, model)
        if efforts := model.efforts.list():
            self.assertEqual((await catalog.resolve(efforts[0])).effort, efforts[0])

        use_temporary = Capability.TEMPORARY in capabilities
        if Capability.CHAT not in capabilities or (
            not use_temporary and Capability.DELETE not in capabilities
        ):
            self.context.skip("models.send: unavailable_or_cannot_safely_clean_up")
            return

        if (
            self.context.account.provider == "chatgpt"
            and os.environ.get("WEBAI_LIVE_ALL_MODELS") != "1"
        ):
            gpt6 = [item for item in models if item.id == "gpt-6" or item.id.startswith("gpt-6-")]
            if gpt6:
                models = gpt6

        for model in models:
            marker = f"{self.context.marker}-m-{model.id}"
            entry = None if use_temporary else self.context.new_remote("chat", marker)
            if entry is not None:
                entry["attempted"] = True
                self.context.save()
            chat = await self.context.session.chats.create(title=marker, temporary=use_temporary)
            try:
                if entry is not None:
                    self.context.record_chat_id(entry, chat)
                efforts = model.efforts.list()
                selected_effort = (
                    next((item for item in efforts if item.id == "standard"), efforts[0])
                    if efforts
                    else None
                )
                op = chat.responses.create(
                    "Reply with only: OK",
                    model=model,
                    **({"effort": selected_effort} if selected_effort is not None else {}),
                )
                response = await self.call_or_skip_rate_limit(
                    f"models.send[{model.id}]", op.start, entry=entry, chat=chat
                )
                if entry is not None:
                    self.context.record_chat_id(entry, chat)
                self.assertTrue(
                    getattr(response, "text", "").strip(),
                    f"Model {model.id} returned empty text",
                )
            finally:
                if entry is not None:
                    await self.context.cleanup_entry_async(entry)
                elif chat.remote_id:
                    with suppress(RemoteNotFound):
                        await self.call_api(
                            "models.delete_temporary",
                            chat.delete,
                            expected_errors=(RemoteNotFound,),
                        )
