from ...accounts.cookie_policy import CookiePolicy

COOKIE_POLICY = CookiePolicy(
    origin="https://grok.com",
    required="sso",
    names=frozenset({"sso", "sso-rw"}),
    secure_names=frozenset({"sso", "sso-rw"}),
    alternatives=("sso-rw",),
    critical_names=("sso-rw", "__cf_bm", "cf_clearance"),
)
