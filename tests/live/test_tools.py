"""Live checks for v2.1 provider tools (WebSearch, ImageGeneration, DeepResearch)."""

from __future__ import annotations

import os
from contextlib import suppress

from webaiprovider import (
    DeepResearchTool,
    ImageGenerationTool,
    ResearchStatus,
    WebSearchTool,
)
from webaiprovider.base import Capability
from webaiprovider.errors import RemoteNotFound

from .support.case import LiveAsyncTestCase, LiveSyncTestCase

_IMAGE_MAGIC_PREFIXES = (
    b"\x89PNG\r\n\x1a\n",
    b"\xff\xd8\xff",
    b"RIFF",
    b"GIF87a",
    b"GIF89a",
)


def _create_tool_chat_steps(case, *, label_prefix: str, allow_temporary: bool = True):
    capabilities = case.context.session.capabilities
    use_temporary = allow_temporary and (Capability.TEMPORARY in capabilities)
    if Capability.CHAT not in capabilities or (
        not use_temporary and Capability.DELETE not in capabilities
    ):
        case.skipTest("Provider cannot create and safely clean up live tool chats")

    marker = f"{case.context.marker}-{label_prefix}"
    entry = None if use_temporary else case.context.new_remote("chat", marker)
    chat = yield case.context.session.chats.create(title=marker, temporary=use_temporary)
    if entry is not None:
        case.context.record_chat_id(entry, chat)
    return entry, chat, marker


def _cleanup_tool_chat_steps(case, entry, chat, delete_label: str):
    if entry is not None:
        yield case.cleanup_entry(entry)
    elif chat is not None and chat.remote_id:
        with suppress(RemoteNotFound):
            yield case.call_api(
                delete_label,
                chat.delete,
                expected_errors=(RemoteNotFound,),
            )


def _web_search_steps(case):
    capabilities = case.context.session.capabilities
    if Capability.WEB_SEARCH not in capabilities:
        case.skipTest("Provider does not support WebSearchTool")

    entry, chat, marker = yield from _create_tool_chat_steps(case, label_prefix="web")
    try:
        prompt = (
            "Search the web for the official Python 3.13.0 release date "
            "and cite the source URL in one short sentence."
        )
        if entry is not None:
            prompt = f"{marker} {prompt}"
        op = chat.responses.create(prompt, tool=WebSearchTool())
        response = yield case.call_or_skip_rate_limit(
            "tools.web_search", op.start, entry=entry, chat=chat
        )
        if entry is not None:
            case.context.record_chat_id(entry, chat)
        case.assertTrue(response.text.strip(), "WebSearchTool returned empty text")
        case.assertTrue(response.sources, "WebSearchTool returned no sources")
        for source in response.sources:
            case.assertTrue(
                source.url.startswith(("http://", "https://")),
                f"Unexpected WebSearch source URL: {source.url!r}",
            )
    finally:
        yield from _cleanup_tool_chat_steps(case, entry, chat, "tools.web_search.delete_temporary")


def _image_generation_steps(case):
    capabilities = case.context.session.capabilities
    if Capability.IMAGE_GENERATION not in capabilities:
        case.skipTest("Provider does not support ImageGenerationTool")

    # ChatGPT does not support AI image generation inside temporary chats.
    allow_temporary = case.context.account.provider != "chatgpt"
    entry, chat, marker = yield from _create_tool_chat_steps(
        case, label_prefix="img", allow_temporary=allow_temporary
    )
    try:
        prompt = "Generate a minimal flat red circle on a plain white background."
        if entry is not None:
            prompt = f"{marker} {prompt}"
        op = chat.responses.create(prompt, tool=ImageGenerationTool())
        response = yield case.call_or_skip_rate_limit(
            "tools.image_generation", op.start, entry=entry, chat=chat
        )
        if entry is not None:
            case.context.record_chat_id(entry, chat)
        artifacts = response.artifacts.list()
        images = [
            artifact
            for artifact in artifacts
            if artifact.kind == "image"
            or (getattr(artifact, "mime_type", None) or "").startswith("image/")
            or (getattr(artifact, "name", None) or "")
            .lower()
            .endswith((".png", ".jpg", ".jpeg", ".webp", ".gif"))
        ]
        case.assertTrue(images, "ImageGenerationTool returned no image artifacts")
        image_bytes = yield case.call_api(
            "tools.image_generation.download",
            lambda: chat.download(images[0]),
        )
        case.assertGreater(len(image_bytes), 100, "Generated image payload is unexpectedly small")
        case.assertTrue(
            image_bytes.startswith(_IMAGE_MAGIC_PREFIXES),
            f"Generated image did not start with a known image header: {image_bytes[:16]!r}",
        )
    finally:
        yield from _cleanup_tool_chat_steps(
            case, entry, chat, "tools.image_generation.delete_temporary"
        )


def _deep_research_steps(case):
    if os.environ.get("WEBAI_LIVE_DEEP_RESEARCH") != "1":
        case.skipTest("Set WEBAI_LIVE_DEEP_RESEARCH=1 to run live DeepResearch checks")
    capabilities = case.context.session.capabilities
    if Capability.DEEP_RESEARCH not in capabilities:
        case.skipTest("Provider does not support DeepResearchTool")

    tool = DeepResearchTool()
    entry, chat, marker = yield from _create_tool_chat_steps(case, label_prefix="dr")
    try:
        prompt = "Summarize the key syntax rules of PEP 8 using only peps.python.org in 3 short bullet points."
        if entry is not None:
            prompt = f"{marker} {prompt}"
        op = chat.responses.create(prompt, tool=tool)
        response = yield case.call_or_skip_rate_limit(
            "tools.deep_research.plan", op.start, entry=entry, chat=chat
        )
        if entry is not None:
            case.context.record_chat_id(entry, chat)
        case.assertIsNotNone(response.research, "DeepResearchTool returned no research state")
        assert response.research is not None
        case.assertIn(
            response.research.status,
            (ResearchStatus.WAITING_FOR_APPROVAL, ResearchStatus.COMPLETED),
        )
        if response.research.status is ResearchStatus.WAITING_FOR_APPROVAL:
            case.assertIsNotNone(
                response.research.plan, "DeepResearch plan missing while waiting for approval"
            )
            assert response.research.plan is not None
            case.assertTrue(response.research.plan.steps, "DeepResearch plan returned zero steps")
            if os.environ.get("WEBAI_LIVE_DEEP_RESEARCH") != "1":
                case.context.skip(
                    "tools.deep_research.execute: opt-in (WEBAI_LIVE_DEEP_RESEARCH=1)"
                )
                return
            response = yield case.call_or_skip_rate_limit(
                "tools.deep_research.execute",
                lambda: response.start_research(timeout=300.0),
                entry=entry,
                chat=chat,
            )
        case.assertIsNotNone(response.research)
        assert response.research is not None
        case.assertEqual(response.research.status, ResearchStatus.COMPLETED)
        docs = [artifact for artifact in response.artifacts.list() if artifact.kind == "document"]
        report_text = (
            (yield case.call_api("tools.deep_research.read_report", docs[0].read_text))
            if docs
            else response.text
        )
        case.assertGreater(len(report_text.strip()), 100, "DeepResearch report is too short")
    finally:
        yield from _cleanup_tool_chat_steps(
            case, entry, chat, "tools.deep_research.delete_temporary"
        )


class AsyncToolApiTests(LiveAsyncTestCase):
    async def test_web_search_tool(self) -> None:
        await self.run_steps(_web_search_steps(self))

    async def test_image_generation_tool(self) -> None:
        await self.run_steps(_image_generation_steps(self))

    async def test_deep_research_tool(self) -> None:
        await self.run_steps(_deep_research_steps(self))


class SyncToolApiTests(LiveSyncTestCase):
    def test_web_search_tool(self) -> None:
        self.run_steps(_web_search_steps(self))

    def test_image_generation_tool(self) -> None:
        self.run_steps(_image_generation_steps(self))

    def test_deep_research_tool(self) -> None:
        self.run_steps(_deep_research_steps(self))
