from ...accounts.cookie_policy import CookiePolicy

COOKIE_POLICY = CookiePolicy(
    origin="https://claude.ai",
    required="sessionKey",
    names=frozenset(
        {"__cf_bm", "anthropic-device-id", "cf_clearance", "lastActiveOrg", "sessionKey"}
    ),
    allow_chunks=True,
    critical_names=("__cf_bm", "cf_clearance"),
)
