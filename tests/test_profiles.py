from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from subprocess import CompletedProcess, TimeoutExpired
from types import SimpleNamespace
from unittest.mock import patch

from webaiprovider.accounts import profiles
from webaiprovider.accounts.cookies import import_cookies
from webaiprovider.accounts.profiles import BrowserProfile, snapshot_sqlite_file
from webaiprovider.errors import (
    ProfileError,
    ProfileInvalidResultError,
    ProfileNotFoundError,
    ProfileSessionNotFoundError,
    ProfileTimeoutError,
)


class BrowserProfileTests(unittest.TestCase):
    def test_chromium_uses_last_profile_and_rejects_traversal(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            root = home / ".config/google-chrome"
            state = root / "Local State"
            state.parent.mkdir(parents=True)
            state.write_text(json.dumps({"profile": {"last_used": "Profile 2"}}))
            cookies = root / "Profile 2/Network/Cookies"
            cookies.parent.mkdir(parents=True)
            cookies.touch()

            with (
                patch.object(profiles.Path, "home", return_value=home),
                patch.object(profiles, "_default_id", return_value="google-chrome"),
                patch.object(profiles.sys, "platform", "linux"),
                patch.dict("os.environ", {"XDG_CONFIG_HOME": str(home / ".config")}),
            ):
                profile = profiles.default_profile()
                self.assertEqual(profile.family, "chromium")
                self.assertEqual(profile.cookies, cookies)
                self.assertEqual(profile.key_file, state)
                self.assertEqual(profiles.list_profiles(), [profile])

                state.write_text(json.dumps({"profile": {"last_used": "../outside"}}))
                with self.assertRaises(ProfileNotFoundError):
                    profiles.default_profile()

    def test_firefox_default_profile_comes_from_profiles_ini(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            root = home / ".mozilla/firefox"
            profile_dir = root / "Profiles/default"
            profile_dir.mkdir(parents=True)
            (profile_dir / "cookies.sqlite").touch()
            (root / "profiles.ini").write_text(
                "[Profile0]\nName=default\nIsRelative=1\nPath=Profiles/default\nDefault=1\n",
                encoding="utf-8",
            )

            with (
                patch.object(profiles.Path, "home", return_value=home),
                patch.object(profiles, "_default_id", return_value="firefox"),
                patch.object(profiles.sys, "platform", "linux"),
            ):
                profile = profiles.default_profile()

            self.assertEqual(profile.family, "firefox")
            self.assertEqual(profile.cookies, profile_dir / "cookies.sqlite")
            self.assertIsNone(profile.key_file)

    def test_firefox_ambiguous_default_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "profiles.ini").write_text(
                "[Profile0]\nPath=one\nDefault=1\n[Profile1]\nPath=two\nDefault=1\n",
                encoding="utf-8",
            )

            with self.assertRaises(ProfileError):
                profiles._firefox_profile(root)

    def test_firefox_cookie_rows_exclude_containers_and_other_hosts(self):
        with tempfile.TemporaryDirectory() as directory:
            cookie_file = Path(directory) / "cookies.sqlite"
            with closing(sqlite3.connect(cookie_file)) as database, database:
                database.execute(
                    "CREATE TABLE moz_cookies "
                    "(host TEXT, path TEXT, isSecure INTEGER, expiry INTEGER, "
                    "name TEXT, value TEXT, isHttpOnly INTEGER, originAttributes TEXT)"
                )
                database.executemany(
                    "INSERT INTO moz_cookies VALUES (?, '/', 1, 0, 'sessionKey', ?, 1, ?)",
                    [
                        (".claude.ai", "account", ""),
                        (".claude.ai", "container", "^userContextId=1"),
                        (".example.com", "other", ""),
                    ],
                )

            rows = profiles._firefox_rows(cookie_file, "claude")

        self.assertEqual([row[5] for row in rows], ["account"])

    def test_firefox_session_backup_filters_other_sites_and_private_contexts(self):
        with tempfile.TemporaryDirectory() as directory:
            profile_dir = Path(directory)
            backup = profile_dir / "sessionstore.js"
            backup.write_text(
                json.dumps(
                    {
                        "cookies": [
                            {"host": ".claude.ai", "name": "sessionKey", "value": "ok"},
                            {"host": ".example.com", "name": "sessionKey", "value": "other"},
                            {
                                "host": ".claude.ai",
                                "name": "sessionKey",
                                "value": "private",
                                "userContextId": 1,
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )

            cookies = profiles._firefox_session_cookies(profile_dir, "claude")

        self.assertEqual([cookie["value"] for cookie in cookies], ["ok"])

    def test_sqlite_snapshot_copies_sidecars_and_cleans_up(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "cookies.sqlite"
            source.write_bytes(b"database")
            source.with_name(source.name + "-wal").write_bytes(b"wal")
            source.with_name(source.name + "-journal").write_bytes(b"journal")

            snapshot, temp_dir = snapshot_sqlite_file(source)
            snapshot_dir = Path(temp_dir.name)
            try:
                self.assertEqual(snapshot.read_bytes(), b"database")
                self.assertEqual(snapshot.with_name(snapshot.name + "-wal").read_bytes(), b"wal")
                self.assertEqual(
                    snapshot.with_name(snapshot.name + "-journal").read_bytes(), b"journal"
                )
            finally:
                temp_dir.cleanup()

            self.assertFalse(snapshot_dir.exists())


class CookieImportBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.profile = BrowserProfile(
            "chromium", "chromium", Path("/profile/Cookies"), Path("/profile/Local State")
        )

    @patch("webaiprovider.accounts.cookies.subprocess.run")
    def test_import_passes_profile_to_isolated_reader_and_parses_credentials(self, run):
        run.return_value = CompletedProcess([], 0, '{"sessionKey":"fixture-cookie"}', "")

        family, cookies = import_cookies("claude", profile=self.profile)

        payload = json.loads(run.call_args.kwargs["input"])
        self.assertEqual(family, "chromium")
        self.assertEqual(cookies, {"sessionKey": "fixture-cookie"})
        self.assertEqual(payload["cookies"], str(self.profile.cookies))
        self.assertEqual(payload["key_file"], str(self.profile.key_file))
        self.assertEqual(run.call_args.kwargs["timeout"], 25)

    def test_import_rejects_missing_session_and_invalid_output(self):
        with (
            patch(
                "webaiprovider.accounts.cookies.subprocess.run",
                return_value=SimpleNamespace(returncode=2, stdout=""),
            ),
            self.assertRaises(ProfileSessionNotFoundError),
        ):
            import_cookies("claude", profile=self.profile)

        with (
            patch(
                "webaiprovider.accounts.cookies.subprocess.run",
                return_value=SimpleNamespace(returncode=0, stdout="[]"),
            ),
            self.assertRaises(ProfileInvalidResultError),
        ):
            import_cookies("claude", profile=self.profile)

    def test_import_translates_reader_timeout(self):
        with (
            patch(
                "webaiprovider.accounts.cookies.subprocess.run",
                side_effect=TimeoutExpired("reader", 25),
            ),
            self.assertRaises(ProfileTimeoutError),
        ):
            import_cookies("claude", profile=self.profile)
