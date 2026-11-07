from __future__ import annotations

import asyncio
import importlib.metadata
import importlib.util
import tempfile
from pathlib import Path

import webaiprovider
from webaiprovider import Account, ChatGPTProvider
from webaiprovider.providers.chatgpt.sentinel import SentinelRuntime, generate_enforcement_proof


def main():
    if importlib.metadata.version("webai-provider") != webaiprovider.__version__:
        raise SystemExit("package metadata and import version do not match")
    if importlib.util.find_spec("webai_provider") is not None:
        raise SystemExit("legacy package namespace is present in the wheel")

    with SentinelRuntime(user_agent="Mozilla/5.0 (X11; Linux x86_64)") as runtime:
        initial = runtime.create_initial_proof()
        if not initial.startswith("gAAAAAC") or not initial.endswith("~S"):
            raise SystemExit("SentinelRuntime did not produce a valid initial proof token")
        proof = generate_enforcement_proof(
            initial,
            {
                "proofofwork": {
                    "required": True,
                    "seed": "smoke-seed",
                    "difficulty": "f",
                }
            },
        )
        if not proof.startswith("gAAAAAB") or not proof.endswith("~S"):
            raise SystemExit("generate_enforcement_proof did not produce a valid proof token")

    async def verify_v2_session():
        with tempfile.TemporaryDirectory() as folder:
            session_file = Path(folder) / "session.json"
            session_file.write_text('{"token": "smoke-test"}', encoding="utf-8")
            acc = Account.from_cookies("chatgpt", session_file)
            if acc.provider != "chatgpt":
                raise SystemExit("Account did not preserve provider identity")
            provider = ChatGPTProvider(account=acc)
            session = provider.connect()
            if session.provider != "chatgpt":
                raise SystemExit("ProviderSession did not preserve provider identity")
            await session.close()

    asyncio.run(verify_v2_session())


if __name__ == "__main__":
    main()
