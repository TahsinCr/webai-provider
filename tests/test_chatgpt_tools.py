from __future__ import annotations

import unittest

from webaiprovider import (
    Artifact,
    Capability,
    ChatGPTWebAIProvider,
    DeepResearchTool,
    ImageGenerationTool,
    MemoryCredentialStore,
    ResearchStatus,
    WebAIAccount,
    WebSearchTool,
)
from webaiprovider.providers.chatgpt.base import BaseChatGPTClient
from webaiprovider.providers.chatgpt.deep_research import (
    CALL_MCP_PATH,
    build_chatgpt_research_response,
    call_mcp_payload,
    parse_deep_research_get_state,
    parse_venus_plan_message,
)
from webaiprovider.providers.chatgpt.formats import emit_post_send_events
from webaiprovider.providers.chatgpt.image import extract_chatgpt_image_artifacts
from webaiprovider.resources.message.protocol import (
    MessageMetadata,
    ProviderRequest,
    ResearchPlanReceived,
)
from webaiprovider.resources.response.response import Response


class ChatGPTToolsTests(unittest.TestCase):
    def test_chatgpt_capabilities_include_all_three_tools(self) -> None:
        account = WebAIAccount(
            id="cgpt-test",
            provider="chatgpt",
            credentials=MemoryCredentialStore({"session": "ok"}),
        )
        provider = ChatGPTWebAIProvider(account)
        self.assertIn(Capability.WEB_SEARCH, provider.capabilities)
        self.assertIn(Capability.IMAGE_GENERATION, provider.capabilities)
        self.assertIn(Capability.DEEP_RESEARCH, provider.capabilities)

    def test_finalize_send_payload_sets_tool_hints(self) -> None:
        for tool, expected_hint in (
            (WebSearchTool(), "search"),
            (ImageGenerationTool(), "picture_v2"),
            (DeepResearchTool(), "plugin:connector_openai_deep_research"),
        ):
            headers: dict[str, str] = {}
            body: dict = {
                "messages": [
                    {
                        "id": "m1",
                        "content": {"content_type": "text", "parts": ["hello"]},
                        "metadata": {},
                    }
                ]
            }
            BaseChatGPTClient._finalize_send_payload(
                headers,
                body,
                None,
                "m1",
                model="gpt-5",
                effort=None,
                temporary=False,
                tool=tool,
                prompt="hello",
            )
            self.assertEqual(body["system_hints"], [expected_hint])
            self.assertEqual(body["messages"][0]["metadata"]["system_hints"], [expected_hint])
            if isinstance(tool, DeepResearchTool):
                self.assertEqual(body["model_response_contracts"], [{"id": "web.mica.v1"}])
                self.assertIn(
                    "connector_openai_deep_research", body["messages"][0]["content"]["parts"][0]
                )

    def test_image_asset_pointer_extraction_and_empty_parts_emit(self) -> None:
        tool_msg = {
            "id": "m-tool",
            "author": {"role": "tool"},
            "channel": "final",
            "content": {
                "content_type": "multimodal_text",
                "parts": [
                    {
                        "content_type": "image_asset_pointer",
                        "asset_pointer": "sediment://file_00000000abc",
                        "mime_type": "image/png",
                        "size_bytes": 12345,
                        "width": 1024,
                        "height": 1024,
                    }
                ],
            },
        }
        from webaiprovider.providers.chatgpt.formats import text_from_parts

        self.assertEqual(text_from_parts(tool_msg["content"]["parts"]), "")
        images = extract_chatgpt_image_artifacts(tool_msg)
        self.assertEqual(len(images), 1)
        self.assertEqual(images[0]["media_type"], "image/png")
        self.assertEqual(images[0]["width"], 1024)

        events: list[object] = []
        emit_post_send_events(
            "conv-1",
            {
                "messages": [
                    {"id": "u1", "author": {"role": "user"}},
                    tool_msg,
                    {
                        "id": "a1",
                        "author": {"role": "assistant"},
                        "content": {"content_type": "reasoning_recap", "content": ""},
                    },
                ]
            },
            previous=None,
            request=ProviderRequest("draw a cat", tool=ImageGenerationTool()),
            publish=lambda item: events.append(item[1]) if item[0] == "event" else None,
        )
        meta = next(e for e in events if isinstance(e, MessageMetadata))
        self.assertEqual(meta.parts, ())
        self.assertEqual(meta.files[0]["remote_url"], "sediment://file_00000000abc")

    def test_deep_research_venus_plan_mcp_and_get_state_parsing(self) -> None:
        plan_msg = {
            "id": "m-plan",
            "author": {"role": "tool", "name": "api_tool.call_tool"},
            "channel": "commentary",
            "metadata": {
                "chatgpt_sdk": {
                    "tool_response_metadata": {
                        "async_task_conversation_id": "task_123",
                        "venus_plan": {
                            "plan_id": "plan_1",
                            "title": "Global AI",
                            "steps": ["Collect releases", "Compare benchmarks"],
                        },
                        "venus_widget_state": {
                            "status": "waiting_for_user_response_on_plan",
                            "countdown_seconds": 51,
                        },
                    }
                }
            },
        }
        state = parse_venus_plan_message(plan_msg, conversation_id="conv-dr")
        assert state is not None and state.plan is not None
        self.assertEqual(state.status, ResearchStatus.WAITING_FOR_APPROVAL)
        self.assertEqual(state.session_id, "task_123")
        self.assertEqual(state.plan.steps, ("Collect releases", "Compare benchmarks"))

        events: list[object] = []
        emit_post_send_events(
            "conv-dr",
            {"messages": [{"id": "u1", "author": {"role": "user"}}, plan_msg]},
            previous=None,
            request=ProviderRequest("research AI", tool=DeepResearchTool()),
            publish=lambda item: events.append(item[1]) if item[0] == "event" else None,
        )
        self.assertTrue(any(isinstance(e, ResearchPlanReceived) for e in events))

        mcp = call_mcp_payload("skip_sleep", "task_123", conversation_id="conv-dr")
        self.assertEqual(CALL_MCP_PATH, "/backend-api/ecosystem/call_mcp")
        self.assertEqual(mcp["params"]["name"], "skip_sleep")
        self.assertEqual(mcp["params"]["arguments"]["session_id"], "task_123")

        get_state_payload = {
            "_meta": {
                "deep_research_widget_messages": [
                    {
                        "content": {
                            "content_type": "thoughts",
                            "thoughts": [{"summary": "Checked docs"}],
                        }
                    },
                    {
                        "channel": "final",
                        "content": {"content_type": "text", "parts": ["# Final Report\n\nBody"]},
                    },
                ],
                "source_searches": [
                    {"results": [{"url": "https://openai.com/news", "title": "OpenAI News"}]}
                ],
            }
        }
        parsed = parse_deep_research_get_state(get_state_payload)
        assert parsed is not None
        resp = build_chatgpt_research_response(
            None, Response("plan", research=state), state, parsed
        )
        assert resp.research is not None
        self.assertEqual(resp.research.status, ResearchStatus.COMPLETED)
        self.assertEqual(len(resp.artifacts), 1)
        self.assertIsInstance(resp.artifacts[0], Artifact)
        self.assertEqual(resp.artifacts[0].mime_type, "text/markdown")
        self.assertIn("# Final Report", resp.artifacts[0].read_text())
        self.assertEqual(resp.sources[0].url, "https://openai.com/news")


if __name__ == "__main__":
    unittest.main()
