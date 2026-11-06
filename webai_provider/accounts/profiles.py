from __future__ import annotations

import configparser
import json
import os
import plistlib
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import uuid
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from ..errors import (
    ProfileDatabaseError,
    ProfileDecryptionError,
    ProfileDependencyError,
    ProfileError,
    ProfileInvalidResultError,
    ProfileKeyringError,
    ProfileNotFoundError,
    ProfilePermissionError,
    ProfileSessionNotFoundError,
    ProfileTimeoutError,
)

__all__ = [
    "BrowserProfile",
    "HOSTS",
    "NAMES",
    "AUTH_COOKIE",
    "default_profile",
    "list_profiles",
    "snapshot_sqlite_file",
    "import_cookies",
    "select_cookies",
    "ProfileError",
    "ProfileNotFoundError",
    "ProfilePermissionError",
    "ProfileSessionNotFoundError",
    "ProfileDependencyError",
    "ProfileDatabaseError",
    "ProfileKeyringError",
    "ProfileDecryptionError",
    "ProfileTimeoutError",
    "ProfileInvalidResultError",
]

_PROFILE_ERROR_MAP = {
    2: ProfileSessionNotFoundError,
    3: ProfileDependencyError,
    4: ProfilePermissionError,
    5: ProfileDatabaseError,
    6: ProfileKeyringError,
    7: ProfileDecryptionError,
}

HOSTS = {
    "chatgpt": "chatgpt.com",
    "gemini": "gemini.google.com",
    "claude": "claude.ai",
    "grok": "grok.com",
}

NAMES = {
    "chatgpt": {"oai-did", "cf_clearance", "__cf_bm", "__Host-next-auth.csrf-token"},
    "gemini": {
        "__Secure-1PSID",
        "__Secure-1PSIDTS",
        "__Secure-3PSID",
        "__Secure-3PSIDTS",
        "SID",
        "HSID",
        "SSID",
        "APISID",
        "SAPISID",
        "__Secure-1PAPISID",
        "__Secure-3PAPISID",
    },
    "claude": {"sessionKey", "lastActiveOrg", "anthropic-device-id", "__cf_bm", "cf_clearance"},
    "grok": set(),
}

AUTH_COOKIE = re.compile(r"__Secure-(?:next-auth|authjs)\.session-token(?:\.\d+)?\Z")
_GROK_AUTH_COOKIE = re.compile(r"(?:^|[_-])(?:auth|session|sso|token)(?:[_-]|$)", re.IGNORECASE)
_GROK_NON_AUTH_COOKIES = {"grok_device_id", "__cf_bm", "cf_clearance"}


def select_cookies(jar, provider):
    from .cookies import select_cookies as _select

    return _select(jar, provider)


def import_cookies(provider):
    from .cookies import import_cookies as _import

    return _import(provider)


def _main():
    from .cookies import _main as _main_impl

    return _main_impl()


def _failure_code(error):
    if isinstance(error, ImportError):
        return 3
    if isinstance(error, PermissionError):
        return 4
    if isinstance(error, sqlite3.Error):
        return 5
    message = str(error).lower()
    if any(
        token in message
        for token in (
            "unable to read database",
            "not a chromium",
            "not a firefox",
            "not a safari",
        )
    ):
        return 5
    if type(error).__module__.startswith(("jeepney", "dbus", "secretstorage")) or any(
        token in message
        for token in (
            "can not find secret",
            "kde wallet",
            "dbus_session_bus_address",
            "org.freedesktop.secret",
        )
    ):
        return 6
    if any(token in message for token in ("decrypt", "dpapi", "app-bound", "encrypted key")):
        return 7
    return 1


@dataclass(frozen=True)
class BrowserProfile:
    family: str
    cookie_backend: str | None
    cookies: Path
    key_file: Path | None = None


@dataclass(frozen=True)
class _BrowserSource:
    family: str
    cookie_backend: str | None
    location: str


_BROWSER_SOURCES = (
    (("brave-origin", "brave.origin"), _BrowserSource("chromium", "brave", "brave-origin")),
    (("brave",), _BrowserSource("chromium", "brave", "brave")),
    (("zen",), _BrowserSource("firefox", None, "zen")),
    (("firefox",), _BrowserSource("firefox", None, "firefox")),
    (
        ("msedge", "microsoft-edge", "com.microsoft.edge", "edg."),
        _BrowserSource("chromium", "edge", "edge"),
    ),
    (("chromium",), _BrowserSource("chromium", "chromium", "chromium")),
    (("chrome",), _BrowserSource("chromium", "chrome", "chrome")),
    (("vivaldi",), _BrowserSource("chromium", "vivaldi", "vivaldi")),
    (("safari",), _BrowserSource("safari", "safari", "safari")),
)

_CHROMIUM_PROFILE_ROOTS = {
    "chrome": {
        "win32": "Google/Chrome/User Data",
        "darwin": "Google/Chrome",
        "linux": "google-chrome",
    },
    "chromium": {
        "win32": "Chromium/User Data",
        "darwin": "Chromium",
        "linux": "chromium",
    },
    "brave": {
        "win32": "BraveSoftware/Brave-Browser/User Data",
        "darwin": "BraveSoftware/Brave-Browser",
        "linux": "BraveSoftware/Brave-Browser",
    },
    "brave-origin": {
        "win32": "BraveSoftware/Brave-Origin/User Data",
        "darwin": "BraveSoftware/Brave-Origin",
        "linux": "BraveSoftware/Brave-Origin",
    },
    "edge": {
        "win32": "Microsoft/Edge/User Data",
        "darwin": "Microsoft Edge",
        "linux": "microsoft-edge",
    },
    "vivaldi": {
        "win32": "Vivaldi/User Data",
        "darwin": "Vivaldi",
        "linux": "vivaldi",
    },
}


def _read_json(path):
    try:
        limit = 8 * 1024 * 1024
        with path.open("rb") as stream:
            size = os.fstat(stream.fileno()).st_size
            if size > limit:
                return {}
            data = stream.read(size)
            if stream.read(1):
                return {}
        value = json.loads(data.decode("utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _default_id():
    if sys.platform == "win32":
        import winreg

        with winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"Software\Microsoft\Windows\Shell\Associations\UrlAssociations\https\UserChoice",
        ) as key:
            return winreg.QueryValueEx(key, "ProgId")[0].lower()
    if sys.platform == "darwin":
        result = subprocess.run(
            ["defaults", "export", "com.apple.LaunchServices/com.apple.launchservices.secure", "-"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=5,
            check=True,
        )
        for item in plistlib.loads(result.stdout).get("LSHandlers", []):
            if item.get("LSHandlerURLScheme") == "https":
                return item.get("LSHandlerRoleAll", "").lower()
        return ""
    result = subprocess.run(
        ["xdg-settings", "get", "default-web-browser"],
        capture_output=True,
        text=True,
        timeout=5,
        check=True,
    )
    return result.stdout.strip().lower()


def _safari_cookie_file(home):
    library = home / "Library/Containers/com.apple.Safari/Data/Library"
    try:
        result = subprocess.run(
            ["defaults", "read", "com.apple.Safari", "SafariProfilesLastActiveProfileUUIDString"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        result = None

    active_profile = result.stdout.strip() if result and result.returncode == 0 else ""
    if active_profile and active_profile.casefold() != "defaultprofile":
        try:
            profile_id = str(uuid.UUID(active_profile)).lower()
        except ValueError:
            raise ProfileNotFoundError("Son kullanılan Safari profili belirlenemedi") from None
        cookies = library / "WebKit/WebsiteDataStore" / profile_id / "Cookies/Cookies.binarycookies"
        if cookies.is_file():
            return cookies
        raise ProfileNotFoundError("Son kullanılan Safari profilinin çerez dosyası bulunamadı")

    if not active_profile:
        profile_store = library / "WebKit/WebsiteDataStore"
        try:
            has_named_profiles = any(
                path.is_dir()
                for path in profile_store.glob("*")
                if _is_safari_profile_id(path.name)
            )
        except OSError:
            raise ProfileError(
                "Safari profili belirlenemedi; Safari'de kullanacağınız profili açın"
            ) from None
        if has_named_profiles:
            raise ProfileError(
                "Safari profili belirlenemedi; Safari'de kullanacağınız profili açın"
            )

    cookie_files = (
        library / "Cookies/Cookies.binarycookies",
        home / "Library/Cookies/Cookies.binarycookies",
    )
    cookies = next((path for path in cookie_files if path.is_file()), None)
    if cookies is None:
        raise ProfileNotFoundError("Varsayılan tarayıcı profili bulunamadı")
    return cookies


def _is_safari_profile_id(value):
    try:
        uuid.UUID(value)
    except (AttributeError, ValueError):
        return False
    return True


def _browser_source(name):
    if any(
        channel in name
        for channel in (
            "-beta",
            "-nightly",
            "-dev",
            ".beta",
            ".nightly",
            "technologypreview",
        )
    ):
        raise ProfileError("Varsayılan tarayıcının profili desteklenmiyor")
    return next(
        (source for tokens, source in _BROWSER_SOURCES if any(token in name for token in tokens)),
        None,
    )


def _chromium_root(location, browser_name, home):
    roots = _CHROMIUM_PROFILE_ROOTS[location]
    if sys.platform == "win32":
        base = Path(os.environ.get("LOCALAPPDATA", home / "AppData/Local"))
        return base / roots["win32"]
    if sys.platform == "darwin":
        base = home / "Library/Application Support"
        return base / roots["darwin"]
    if location == "chrome" and browser_name.startswith("com.google.chrome"):
        return home / ".var/app/com.google.Chrome/config/google-chrome"
    base = Path(os.environ.get("XDG_CONFIG_HOME", home / ".config"))
    return base / roots["linux"]


def _firefox_root(location, browser_name, home):
    if sys.platform == "win32":
        base = Path(os.environ.get("APPDATA", home / "AppData/Roaming"))
        return base / ("Mozilla/Firefox" if location == "firefox" else "zen")
    if sys.platform == "darwin":
        base = home / "Library/Application Support"
        return base / ("Firefox" if location == "firefox" else "zen")

    config = Path(os.environ.get("XDG_CONFIG_HOME", home / ".config"))
    if location == "firefox":
        roots = [home / ".mozilla/firefox", home / "snap/firefox/common/.mozilla/firefox"]
        flatpak = home / ".var/app/org.mozilla.firefox/.mozilla/firefox"
        if "flatpak" in browser_name or browser_name.startswith("org.mozilla."):
            roots.insert(0, flatpak)
        if "firefox_firefox" in browser_name:
            roots.reverse()
    else:
        roots = [config / "zen", home / ".zen"]
        if "app.zen_browser" in browser_name:
            sandbox = home / ".var/app/app.zen_browser.zen"
            roots = [sandbox / "config/zen", sandbox / ".zen", sandbox / "zen"]
    return next((root for root in roots if (root / "profiles.ini").is_file()), roots[0])


def default_profile(family: str | None = None) -> BrowserProfile:
    try:
        name = _default_id().lower()
    except (OSError, ValueError, subprocess.SubprocessError):
        raise ProfileError("Varsayılan tarayıcı belirlenemedi") from None
    source = _browser_source(name)
    if source is None:
        raise ProfileError("Varsayılan tarayıcının profili desteklenmiyor")
    if family is not None and source.family != family:
        raise ProfileError(
            f"Varsayılan tarayıcı ({source.family}) istenen aile ({family}) ile eşleşmiyor"
        )
    home = Path.home()
    if source.family == "safari":
        if sys.platform != "darwin":
            raise ProfileError("Safari profilleri yalnızca macOS üzerinde destekleniyor")
        return BrowserProfile("safari", "safari", _safari_cookie_file(home))
    if source.family == "firefox":
        root = _firefox_root(source.location, name, home)
        cookies, key = _firefox_profile(root) / "cookies.sqlite", None
    else:
        root = _chromium_root(source.location, name, home)
        state = _read_json(root / "Local State")
        if (root / "Local State").is_file() and not state:
            raise ProfileNotFoundError("Varsayılan tarayıcı profili bulunamadı")
        profile = state.get("profile", {})
        selected = profile.get("last_used", "Default") if isinstance(profile, dict) else "Default"
        if (
            not isinstance(selected, str)
            or selected in (".", "..")
            or "/" in selected
            or "\\" in selected
        ):
            raise ProfileNotFoundError("Varsayılan tarayıcı profili bulunamadı")
        directory = root / selected
        cookies = directory / "Network/Cookies"
        if not cookies.is_file():
            cookies = directory / "Cookies"
        key = root / "Local State"
    if not cookies.is_file():
        raise ProfileNotFoundError("Varsayılan tarayıcı profili bulunamadı")
    return BrowserProfile(source.family, source.cookie_backend, cookies, key)


def list_profiles() -> list[BrowserProfile]:
    profiles: list[BrowserProfile] = []
    try:
        prof = default_profile()
        profiles.append(prof)
    except ProfileError:
        pass
    return profiles


def _firefox_profile(root):
    parser = configparser.ConfigParser(interpolation=None)
    installs = configparser.ConfigParser(interpolation=None)
    try:
        parser.read(root / "profiles.ini", encoding="utf-8")
        installs.read(root / "installs.ini", encoding="utf-8")
        defaults = {
            parser[s]["Default"]
            for s in parser.sections()
            if s.startswith("Install") and parser[s].get("Default")
        }
        if not defaults:
            defaults = {
                installs[s]["Default"] for s in installs.sections() if installs[s].get("Default")
            }
        if not defaults:
            defaults = {
                parser[s]["Path"]
                for s in parser.sections()
                if s.startswith("Profile") and parser[s].get("Default") == "1"
            }
        if not defaults:
            defaults = {
                parser[s]["Path"]
                for s in parser.sections()
                if s.startswith("Profile") and parser[s].get("Path")
            }
        if len(defaults) != 1:
            raise ProfileError(
                "Varsayılan profil belirlenemedi; tarayıcıda varsayılan profil seçin"
            )
        return root / defaults.pop()
    except (OSError, configparser.Error, KeyError):
        raise ProfileNotFoundError("Varsayılan tarayıcı profili bulunamadı") from None


def snapshot_sqlite_file(
    source_path: Path, prefix: str = "webai-profile-"
) -> tuple[Path, tempfile.TemporaryDirectory]:
    source = Path(source_path)
    sidecars = [source.with_name(source.name + suffix) for suffix in ("-wal", "-journal")]
    for _ in range(3):
        temp_dir = tempfile.TemporaryDirectory(prefix=prefix)
        folder = Path(temp_dir.name)
        try:
            files = [source] + [path for path in sidecars if path.is_file()]
            before = [(p.stat().st_size, p.stat().st_mtime_ns) for p in files]
            target = folder / source.name
            for path in files:
                shutil.copyfile(path, folder / path.name)
            after = [(p.stat().st_size, p.stat().st_mtime_ns) for p in files]
            if before != after or any(path.is_file() and path not in files for path in sidecars):
                temp_dir.cleanup()
                continue
            return target, temp_dir
        except FileNotFoundError:
            temp_dir.cleanup()
            continue
    raise sqlite3.OperationalError("Cookie snapshot changed during copy")


def _firefox_rows(cookie_file, provider):
    from .cookies import HOSTS

    source = Path(cookie_file)

    def read(path, private=False):
        uri = path.resolve().as_uri() + ("?mode=rw" if private else "?mode=ro")
        with closing(sqlite3.connect(uri, uri=True, timeout=0.25)) as database:
            columns = {row[1] for row in database.execute("PRAGMA table_info(moz_cookies)")}
            scope = (
                " AND COALESCE(originAttributes, '') = ''" if "originAttributes" in columns else ""
            )
            host = HOSTS[provider]
            domains = [host, "." + host]
            if provider == "gemini":
                domains.extend(["google.com", ".google.com"])
            query = (
                "SELECT host,path,isSecure,expiry,name,value,isHttpOnly FROM moz_cookies WHERE host IN ("
                + ",".join("?" for _ in domains)
                + ")"
                + scope
            )
            return list(database.execute(query, domains))

    try:
        return read(source)
    except sqlite3.OperationalError:
        pass
    sidecars = [source.with_name(source.name + suffix) for suffix in ("-wal", "-journal")]
    for _ in range(3):
        with tempfile.TemporaryDirectory(prefix="webai-profile-") as folder:
            try:
                files = [source] + [path for path in sidecars if path.is_file()]
                before = [(p.stat().st_size, p.stat().st_mtime_ns) for p in files]
                target = Path(folder) / source.name
                for path in files:
                    shutil.copyfile(path, Path(folder) / path.name)
                after = [(p.stat().st_size, p.stat().st_mtime_ns) for p in files]
                if before != after or any(
                    path.is_file() and path not in files for path in sidecars
                ):
                    continue
                return read(target, private=True)
            except FileNotFoundError:
                continue
    raise sqlite3.OperationalError("Cookie snapshot changed during copy")


def _firefox_session_cookies(directory, provider):
    from .cookies import HOSTS

    candidates = [
        directory / "sessionstore-backups/recovery.jsonlz4",
        directory / "sessionstore.jsonlz4",
        directory / "sessionstore.js",
    ]
    limit = 64 * 1024 * 1024
    try:
        existing = [path for path in candidates if path.is_file()]
        if not existing:
            return []
        path = max(existing, key=lambda item: item.stat().st_mtime_ns)
        with path.open("rb") as stream:
            raw = stream.read(limit + 1)
        if len(raw) > limit:
            return []
        if path.suffix == ".jsonlz4":
            import lz4.block

            if not raw.startswith(b"mozLz40\0") or len(raw) < 12:
                return []
            if int.from_bytes(raw[8:12], "little") > limit:
                return []
            raw = lz4.block.decompress(raw[8:])
        data = json.loads(raw)
        if not isinstance(data, dict):
            return []
        cookies = list(data.get("cookies") or [])
        for window in data.get("windows") or []:
            if isinstance(window, dict):
                cookies.extend(window.get("cookies") or [])
    except (OSError, ValueError, RuntimeError):
        return []
    domains = {HOSTS[provider], "." + HOSTS[provider]}
    if provider == "gemini":
        domains.update({"google.com", ".google.com"})
    return [
        cookie
        for cookie in cookies
        if isinstance(cookie, dict)
        and cookie.get("host") in domains
        and not cookie.get("originAttributes")
        and not cookie.get("userContextId")
        and not cookie.get("privateBrowsingId")
    ]
