from __future__ import annotations

from typing import Any

from webaiprovider import Account
from webaiprovider.resources.model import ModelCatalogState
from webaiprovider.resources.model.model import Model, ModelCatalog


def provider_account(
    provider: str,
    credentials: Any = None,
    account_id: str = "test",
) -> Account:
    return Account(account_id, provider, {} if credentials is None else credentials)


def claude_model_catalog(model_id: str = "model") -> ModelCatalog:
    return ModelCatalog(
        ModelCatalogState.AVAILABLE,
        (Model(model_id, model_id),),
        model_id,
    )
