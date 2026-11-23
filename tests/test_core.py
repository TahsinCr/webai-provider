from __future__ import annotations

import ast
import asyncio
import importlib
import threading
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from webaiprovider.core.cache import BoundedCache
from webaiprovider.core.concurrency import gather_bounded
from webaiprovider.core.hooks import OperationHooks
from webaiprovider.core.types import OperationContext
from webaiprovider.providers.grok.client import GrokClient, GrokProjectEditor
from webaiprovider.providers.grok.records import response_events
from webaiprovider.resources.message.protocol import (
    ConversationBound,
    ProviderRequest,
    ResponseComplete,
    TextDelta,
)
from webaiprovider.resources.operations import execute_with_hooks
from webaiprovider.transport.frames import iter_bounded_lines

_REPO_ROOT = Path(__file__).resolve().parent.parent
_NATIVE_SYNC_MODULES = (
    "webaiprovider/base.py",
    "webaiprovider/provider.py",
    "webaiprovider/core/hooks.py",
    "webaiprovider/transport/http.py",
    "webaiprovider/transport/session.py",
    "webaiprovider/transport/curl_stream.py",
    "webaiprovider/providers/grok/base.py",
    "webaiprovider/providers/grok/client.py",
    "webaiprovider/providers/grok/provider.py",
    "webaiprovider/providers/claude/base.py",
    "webaiprovider/providers/claude/client.py",
    "webaiprovider/providers/claude/provider.py",
    "webaiprovider/providers/gemini/base.py",
    "webaiprovider/providers/gemini/client.py",
    "webaiprovider/providers/gemini/provider.py",
    "webaiprovider/providers/gemini/rpc.py",
    "webaiprovider/providers/gemini/notebooks.py",
    "webaiprovider/providers/gemini/generation.py",
    "webaiprovider/providers/chatgpt/base.py",
    "webaiprovider/providers/chatgpt/client.py",
    "webaiprovider/providers/chatgpt/provider.py",
)
_FORBIDDEN_BRIDGE_SYMBOLS = frozenset(
    {
        "_CallWaiter",
        "_CallEvents",
        "_ProviderTransport",
        "_ProviderStream",
        "_make_async_provider",
        "_bind_sync_events",
        "_sync_events",
        "_sync_event_enqueue",
        "_from_async",
    }
)
_DELETED_LEGACY_MODULES = (
    "webaiprovider/providers/chatgpt/projects.py",
    "webaiprovider/providers/claude/projects.py",
    "webaiprovider/providers/gemini/projects.py",
    "webaiprovider/providers/gemini/session.py",
    "webaiprovider/providers/grok/projects.py",
    "webaiprovider/providers/grok/sync_client.py",
)


class CoreConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_bounded_gather_reuses_workers_and_preserves_input_order(self) -> None:
        tasks = set()

        async def read(index: int) -> int:
            tasks.add(asyncio.current_task())
            if index % 2 == 0:
                await asyncio.sleep(0)
            return index

        self.assertEqual(await gather_bounded(range(32), read, limit=4), list(range(32)))
        self.assertEqual(len(tasks), 4)


class CoreCacheTests(unittest.TestCase):
    def test_cache_evicts_when_capacity_exceeded(self) -> None:
        cache: BoundedCache[str, str] = BoundedCache(
            max_entries=2,
            max_bytes=1000,
        )

        cache.put("k1", "v1")
        cache.put("k2", "value-2")
        cache.put("k3", "v3")

        self.assertIsNone(cache.get("k1"))
        self.assertEqual(cache.get("k2"), "value-2")
        self.assertEqual(cache.get("k3"), "v3")

    def test_cache_evicts_when_byte_limit_exceeded(self) -> None:
        cache: BoundedCache[str, str] = BoundedCache(
            max_entries=10,
            max_bytes=60,
            getsizeof=lambda _key, value: len(value),
        )

        cache.put("k1", "a" * 25)
        cache.put("k2", "b" * 25)
        cache.put("k3", "c" * 25)

        self.assertIsNone(cache.get("k1"))
        self.assertEqual(cache.get("k3"), "c" * 25)

    def test_cache_removal_and_clear(self) -> None:
        cache: BoundedCache[str, str] = BoundedCache(
            max_entries=5,
            max_bytes=1000,
        )
        cache.put("k1", "val1")
        cache.put("k2", "val2")
        self.assertGreater(cache.stats().bytes, 0)
        self.assertEqual(cache.remove("k1"), "val1")
        self.assertIsNone(cache.remove("k1"))
        self.assertEqual(cache.remove("k2"), "val2")

        cache.clear()
        self.assertEqual(cache.stats().entries, 0)

    def test_cache_rejects_oversized_entries_without_evicting_existing(self) -> None:
        cache: BoundedCache[str, str] = BoundedCache(
            max_entries=5,
            max_bytes=20,
            getsizeof=lambda _key, value: len(value),
        )
        self.assertTrue(cache.put("small", "a" * 10))
        self.assertFalse(cache.put("huge", "b" * 25))
        self.assertEqual(cache.get("small"), "a" * 10)
        self.assertIsNone(cache.get("huge"))
        self.assertEqual(cache.stats().entries, 1)
        self.assertEqual(cache.stats().bytes, 10)

    def test_cache_ttl_expiration_and_proactive_cleanup(self) -> None:
        now = 100.0
        cache: BoundedCache[str, str] = BoundedCache(
            max_entries=2,
            max_bytes=100,
            ttl=10.0,
            getsizeof=lambda _key, value: len(value),
            clock=lambda: now,
        )
        cache.put("k1", "v1")
        now = 105.0
        cache.put("k2", "v2")
        now = 111.0
        self.assertIsNone(cache.get("k1"))
        self.assertEqual(cache.get("k2"), "v2")
        cache.put("k3", "v3")
        self.assertEqual(cache.get("k2"), "v2")
        self.assertEqual(cache.get("k3"), "v3")
        self.assertEqual(cache.stats().entries, 2)


class CoreStateMachineTests(unittest.TestCase):
    def test_valid_invalid_and_terminal_transitions(self) -> None:
        from webaiprovider.core.state import (
            _LIFECYCLE_TERMINAL_STATES,
            _LIFECYCLE_TRANSITIONS,
            InvalidTransitionError,
            LifecycleState,
            StateMachine,
        )

        sm = StateMachine(
            initial=LifecycleState.CREATED,
            transitions=_LIFECYCLE_TRANSITIONS,
            terminal=_LIFECYCLE_TERMINAL_STATES,
        )
        self.assertEqual(sm.state, LifecycleState.CREATED)
        self.assertFalse(sm.is_terminal)
        self.assertTrue(sm.can_transition(LifecycleState.OPENING))
        self.assertFalse(sm.can_transition(LifecycleState.OPEN))

        with self.assertRaises(InvalidTransitionError):
            sm.transition(LifecycleState.OPEN)

        self.assertEqual(sm.transition(LifecycleState.OPENING), LifecycleState.OPENING)
        self.assertEqual(sm.transition(LifecycleState.OPEN), LifecycleState.OPEN)
        self.assertEqual(sm.transition(LifecycleState.CLOSING), LifecycleState.CLOSING)
        self.assertEqual(sm.transition(LifecycleState.CLOSED), LifecycleState.CLOSED)
        self.assertTrue(sm.is_terminal)
        self.assertFalse(sm.can_transition(LifecycleState.CLOSING))
        with self.assertRaises(InvalidTransitionError):
            sm.transition(LifecycleState.OPENING)


class NativeSyncFoundationTests(unittest.TestCase):
    def test_native_sync_modules_do_not_import_asyncio(self) -> None:
        for rel_path in _NATIVE_SYNC_MODULES:
            tree = ast.parse((_REPO_ROOT / rel_path).read_text(encoding="utf-8"))
            imports = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imports.update(alias.name.split(".", 1)[0] for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imports.add(node.module.split(".", 1)[0])
            self.assertNotIn("asyncio", imports, f"{rel_path} must not import asyncio")

    def test_no_legacy_sync_async_bridge_symbols_or_files_exist(self) -> None:
        for rel_path in _DELETED_LEGACY_MODULES:
            self.assertFalse((_REPO_ROOT / rel_path).exists(), f"Legacy file remains: {rel_path}")

        for py_file in (_REPO_ROOT / "webaiprovider").rglob("*.py"):
            tree = ast.parse(py_file.read_text(encoding="utf-8"))
            found: set[str] = set()
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    if node.name in _FORBIDDEN_BRIDGE_SYMBOLS:
                        found.add(node.name)
                elif isinstance(node, ast.Name) and node.id in _FORBIDDEN_BRIDGE_SYMBOLS:
                    found.add(node.id)
                elif isinstance(node, ast.Attribute) and node.attr in _FORBIDDEN_BRIDGE_SYMBOLS:
                    found.add(node.attr)
            self.assertEqual(
                found,
                set(),
                f"{py_file.relative_to(_REPO_ROOT)} still references legacy bridge symbols: {sorted(found)}",
            )

    def test_all_four_providers_have_symmetric_base_sync_and_async_architecture(self) -> None:
        for kind, title in (
            ("chatgpt", "ChatGPT"),
            ("claude", "Claude"),
            ("gemini", "Gemini"),
            ("grok", "Grok"),
        ):
            with self.subTest(provider=kind):
                base_mod = importlib.import_module(f"webaiprovider.providers.{kind}.base")
                client_mod = importlib.import_module(f"webaiprovider.providers.{kind}.client")
                async_client_mod = importlib.import_module(
                    f"webaiprovider.providers.{kind}.async_client"
                )
                provider_mod = importlib.import_module(f"webaiprovider.providers.{kind}.provider")
                async_provider_mod = importlib.import_module(
                    f"webaiprovider.providers.{kind}.async_provider"
                )

                base_client = getattr(base_mod, f"Base{title}Client")
                sync_client = getattr(client_mod, f"{title}Client")
                async_client = getattr(async_client_mod, f"Async{title}Client")
                self.assertTrue(issubclass(sync_client, base_client))
                self.assertTrue(issubclass(async_client, base_client))

                base_editor = getattr(base_mod, f"Base{title}ProjectEditor")
                sync_editor = getattr(client_mod, f"{title}ProjectEditor")
                async_editor = getattr(async_client_mod, f"Async{title}ProjectEditor")
                self.assertTrue(issubclass(sync_editor, base_editor))
                self.assertTrue(issubclass(async_editor, base_editor))

                base_provider = getattr(base_mod, f"Base{title}WebAIProvider")
                sync_provider = getattr(provider_mod, f"{title}WebAIProvider")
                async_provider = getattr(async_provider_mod, f"Async{title}WebAIProvider")
                self.assertTrue(issubclass(sync_provider, base_provider))
                self.assertTrue(issubclass(async_provider, base_provider))

    def test_sync_hooks_and_execute_with_hooks_run_on_caller_thread(self) -> None:
        caller = threading.get_ident()
        seen: list[tuple[str, int]] = []
        hooks = OperationHooks()
        hooks.before("demo.op", lambda ctx: seen.append(("before", threading.get_ident())))
        hooks.on_success("demo.op", lambda ctx: seen.append(("success", threading.get_ident())))
        hooks.on_finally("demo.op", lambda ctx: seen.append(("finally", threading.get_ident())))
        ctx = OperationContext(
            name="demo.op", operation_id="op1", provider_id="p1", account_id="a1"
        )

        result = execute_with_hooks(
            operation_name="demo.op",
            context=ctx,
            hooks_chain=(hooks,),
            work=lambda _c: (seen.append(("work", threading.get_ident())), "ok")[1],
        )

        self.assertEqual(result, "ok")
        self.assertEqual(
            seen,
            [("before", caller), ("work", caller), ("success", caller), ("finally", caller)],
        )

    def test_iter_bounded_lines_decodes_split_chunks(self) -> None:
        lines = list(iter_bounded_lines([b"data: {", b'"a":1}\r\n\nline2']))
        self.assertEqual(lines, ['data: {"a":1}', "", "line2"])

    def test_sync_grok_client_and_stream_run_on_caller_thread(self) -> None:
        caller = threading.get_ident()
        threads: list[int] = []

        class StubTransport:
            origin = "https://grok.com"

            def request(self, method: str, path: str, **_kwargs):
                threads.append(threading.get_ident())
                if path == "/rest/user-settings":
                    return {"userId": "u1"}
                if method == "PUT" and path == "/rest/workspaces/ws-1":
                    return {"workspaceId": "ws-1", "name": "Renamed"}
                raise AssertionError((method, path))

            @contextmanager
            def stream(self, _method: str, _path: str, **_kwargs):
                threads.append(threading.get_ident())
                yield SimpleNamespace(
                    iter_bytes=lambda: iter(
                        [
                            b'{"result":{"conversation":{"conversationId":"c-1"},'
                            b'"response":{"token":"Hi","responseId":"r-1"}}}\n',
                            b'data: {"result":{"response":{"done":true}}}\n\n',
                        ]
                    )
                )

            def close(self) -> None:
                pass

        client = GrokClient(Path("/tmp/unused-grok.json"), transport=StubTransport())
        try:
            editor = GrokProjectEditor(client)
            self.assertEqual(editor.update("ws-1", name="Renamed")["name"], "Renamed")
            remembered: list[tuple[str, str]] = []
            events = list(
                response_events(
                    ProviderRequest(prompt="hello"),
                    prepare=lambda _req: (
                        client.events("/rest/app-chat/conversations/new", {}),
                        "grok-3",
                    ),
                    raise_stream_error=lambda err: (_ for _ in ()).throw(RuntimeError(str(err))),
                    remember=lambda cid, rid: remembered.append((cid, rid)),
                )
            )
            self.assertEqual(remembered, [("c-1", "r-1")])
            self.assertTrue(
                any(isinstance(e, ConversationBound) and e.conversation_id == "c-1" for e in events)
            )
            self.assertTrue(any(isinstance(e, TextDelta) and e.text == "Hi" for e in events))
            self.assertTrue(any(isinstance(e, ResponseComplete) for e in events))
            self.assertEqual(set(threads), {caller})
        finally:
            client.close()

    def test_all_four_sync_providers_instantiate_natively_on_caller_thread(self) -> None:
        from webaiprovider import WebAIAccount, WebAIProvider
        from webaiprovider.transport import HTTPTransport

        for kind, origin in (
            ("chatgpt", "https://chatgpt.com"),
            ("claude", "https://claude.ai"),
            ("gemini", "https://gemini.google.com"),
            ("grok", "https://grok.com"),
        ):
            with self.subTest(provider=kind):
                transport = HTTPTransport(origin, client_getter=lambda: SimpleNamespace())
                provider = WebAIProvider(WebAIAccount.from_cookies(kind, {}), transport=transport)
                try:
                    self.assertFalse(hasattr(provider, "_loop"))
                    self.assertFalse(hasattr(provider, "_provider"))
                    self.assertFalse(hasattr(provider, "_call"))
                finally:
                    provider.close()


if __name__ == "__main__":
    unittest.main()
