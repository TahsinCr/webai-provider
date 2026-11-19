from ...accounts.cookie_policy import CookiePolicy

COOKIE_POLICY = CookiePolicy(
    origin="https://gemini.google.com",
    required="__Secure-1PSID",
    names=frozenset(
        {
            "APISID",
            "HSID",
            "SAPISID",
            "SID",
            "SSID",
            "__Secure-1PAPISID",
            "__Secure-1PSID",
            "__Secure-1PSIDTS",
            "__Secure-3PAPISID",
            "__Secure-3PSID",
            "__Secure-3PSIDTS",
        }
    ),
    browser_domain="google.com",
)
