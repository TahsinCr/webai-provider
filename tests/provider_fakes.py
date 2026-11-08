from __future__ import annotations

from typing import Any

from webaiprovider import Account


def provider_account(
    provider: str,
    credentials: Any = None,
    account_id: str = "test",
) -> Account:
    return Account(account_id, provider, {} if credentials is None else credentials)
