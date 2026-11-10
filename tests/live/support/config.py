from __future__ import annotations

import json
import os
import re
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import SafetyError

ROOT = Path(__file__).resolve().parents[3]
CONFIG_DIR = ROOT / ".config"
CREDENTIALS_FILE = CONFIG_DIR / "credentials.json"
PROVIDERS = frozenset({"chatgpt", "gemini", "claude", "grok"})
_ACCOUNT_ID = re.compile(r"^[A-Za-z0-9_-]{1,48}$")


@dataclass(frozen=True, repr=False)
class LiveAccount:
    alias: str
    provider: str
    credentials: dict[str, Any]


def _private_directory(path: Path) -> None:
    try:
        info = path.lstat()
    except OSError:
        raise SafetyError("The private .config directory is unavailable") from None
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise SafetyError("The private .config path must be a real directory")
    if os.name == "posix" and (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700):
        raise SafetyError("The private .config directory must be owned by you with mode 0700")


def read_private_json(path: Path, *, require_ignored: bool = False) -> Any:
    _private_directory(path.parent)
    try:
        info = path.lstat()
    except OSError:
        raise SafetyError("A required private JSON file is missing") from None
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise SafetyError("Private JSON files must be regular files, not symlinks")
    if os.name == "posix" and (info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600):
        raise SafetyError("Private JSON files must be owned by you with mode 0600")
    if require_ignored:
        relative = path.relative_to(ROOT).as_posix()
        check = subprocess.run(
            ["git", "check-ignore", "--quiet", "--", relative],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if check.returncode != 0:
            raise SafetyError("The credential file is not excluded from Git")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise SafetyError(
            "Private JSON file is unreadable or malformed; contents were not printed"
        ) from None


def load_accounts(
    path: Path = CREDENTIALS_FILE, *, require_ignored: bool = True
) -> tuple[LiveAccount, ...]:
    return parse_accounts(read_private_json(path, require_ignored=require_ignored))


def parse_accounts(data: Any) -> tuple[LiveAccount, ...]:
    if not isinstance(data, dict) or data.get("version") != 1:
        raise SafetyError("Credentials JSON must use version 1")
    raw_accounts = data.get("accounts")
    if not isinstance(raw_accounts, list):
        raise SafetyError("Credentials JSON must contain an accounts list")
    accounts: list[LiveAccount] = []
    seen: set[tuple[str, str]] = set()
    for record in raw_accounts:
        if not isinstance(record, dict):
            raise SafetyError("Each account entry must be an object")
        alias, provider, credentials = (
            record.get("id"),
            record.get("provider"),
            record.get("credentials"),
        )
        if not isinstance(alias, str) or not _ACCOUNT_ID.fullmatch(alias):
            raise SafetyError(
                "Account IDs must be short local aliases using letters, digits, _ or -"
            )
        if not isinstance(provider, str) or provider.casefold() not in PROVIDERS:
            raise SafetyError("Each account must name a supported provider")
        if not isinstance(credentials, dict) or not credentials:
            raise SafetyError("Each account must contain a non-empty credentials object")
        provider = provider.casefold()
        if (provider, alias) in seen:
            raise SafetyError("Account aliases must be unique within a provider")
        seen.add((provider, alias))
        accounts.append(LiveAccount(alias, provider, credentials))
    return tuple(accounts)


def select_account(accounts: tuple[LiveAccount, ...], provider: str, alias: str) -> LiveAccount:
    matches = [item for item in accounts if item.provider == provider and item.alias == alias]
    if len(matches) != 1:
        raise SafetyError("Select exactly one configured provider account by its local alias")
    return matches[0]
