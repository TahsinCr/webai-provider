from __future__ import annotations

from urllib.parse import parse_qs, unquote, urlsplit

from ...base import BaseProvider, Capability, ProviderType


class BaseClaudeProvider(BaseProvider):
    """Provider identity and protocol rules shared by both execution modes."""

    provider_type = ProviderType.CLAUDE

    _capabilities = frozenset(
        {
            Capability.CHAT,
            Capability.TEMPORARY,
            Capability.AUTH,
            Capability.HISTORY,
            Capability.MODELS,
            Capability.EFFORT,
            Capability.PROJECTS,
            Capability.PROJECT_CREATE,
            Capability.PROJECT_UPDATE,
            Capability.RESOURCES,
            Capability.PROJECT_DELETE,
            Capability.PROJECT_MEMBERSHIP,
            Capability.DELETE,
            Capability.RENAME,
            Capability.ATTACHMENTS,
            Capability.MESSAGE_EDIT,
            Capability.MESSAGE_RETRY,
        }
    )

    @staticmethod
    def _download_path(url: str) -> str | None:
        parsed = urlsplit(url)
        path = unquote(parsed.path)
        direct = (
            parsed.scheme == "https"
            and parsed.netloc == "claude.ai"
            and parsed.path.endswith("/wiggle/download-file")
        )
        if direct:
            path = parse_qs(parsed.query).get("path", [""])[0]
        elif parsed.scheme not in ("", "sandbox", "computer") or not path.startswith(
            "/mnt/user-data/"
        ):
            return None
        if (parsed.netloc and not direct) or any(part in (".", "..") for part in path.split("/")):
            raise ValueError("Unsupported conversation file path")
        return path
