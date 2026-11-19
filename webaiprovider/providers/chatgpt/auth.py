from ...accounts.cookie_policy import CookiePolicy

COOKIE_POLICY = CookiePolicy(
    origin="https://chatgpt.com",
    required="__Secure-next-auth.session-token",
    names=frozenset(
        {
            "__Host-next-auth.csrf-token",
            "__Secure-next-auth.session-token",
            "__Secure-authjs.session-token",
            "__cf_bm",
            "cf_clearance",
            "oai-did",
        }
    ),
    allow_chunks=True,
    alternatives=("__Secure-authjs.session-token",),
    critical_names=("__cf_bm", "cf_clearance"),
)
