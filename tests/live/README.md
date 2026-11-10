# Live provider API tests

This package tests the public provider API against one real account at a time. It is separate from the deterministic offline suite. Live requests run only when `WEBAI_LIVE_TESTS=1` is explicitly set; otherwise the test class is skipped. There is no CLI runner.

Feature files define test mixins; `test_api.py` composes them into one `unittest.TestCase`. This keeps session setup, rate limiting, and cleanup shared across the API scenarios instead of repeating provider preparation for each feature.

## Account setup

Use the ignored local file `.config/credentials.json`. The file must be owned by your user with mode `0600`, and `.config/` must have mode `0700` on POSIX systems. The test suite rejects symlinks, permissive files, malformed JSON, and credential files that Git does not ignore. Values are plaintext at rest; permissions are not encryption.

```json
{
  "version": 1,
  "accounts": [
    {
      "id": "personal",
      "provider": "chatgpt",
      "credentials": {
        "cookies": {
          "cookie-name": "REPLACE_LOCALLY"
        }
      }
    }
  ]
}
```

Keep the credential payload in the format accepted by the selected provider. `id` is a local alias; credentials are never printed or committed.

## Run one provider and API style

From the repository root, explicitly select one provider account and API style:

```sh
WEBAI_LIVE_TESTS=1 WEBAI_LIVE_PROVIDER=chatgpt WEBAI_LIVE_ACCOUNT=personal WEBAI_LIVE_API=async \
  python -m unittest tests.live.test_api -v
```

Use `WEBAI_LIVE_API=sync` to test the synchronous API. Run providers separately by setting `WEBAI_LIVE_PROVIDER` to `chatgpt`, `gemini`, `claude`, or `grok`. Each run opens one session and shares it across the test modules.

## Coverage and limits

The suite checks session setup, the available model catalog, chat and project metadata, and a disposable project lifecycle (create, rename, instruction update, text resource add/remove, delete verification). It also sends one low-effort prompt in a disposable chat, checks streaming and history, optionally renames the chat, and verifies deletion. Provider capabilities determine which optional actions run.

Calls are serialized with a one-second cooldown between public API actions, a 20-action run limit, and no automatic retries. Providers may make multiple HTTP requests inside one API action, so the harness cannot pace every internal request. Async calls have a 90-second timeout; sync calls use the provider transport timeout. The report contains action names and durations only, never prompts, responses, credentials, or remote IDs.

Before creating a remote test object, the suite writes a private cleanup journal. It deletes only an object whose remote title contains the run's unique random marker, then verifies the deletion. If a provider or network error prevents confirmation, the suite keeps the journal and blocks another run. Recover only the selected account's pending test objects with:

```sh
WEBAI_LIVE_TESTS=1 WEBAI_LIVE_RECOVER=1 WEBAI_LIVE_PROVIDER=chatgpt WEBAI_LIVE_ACCOUNT=personal WEBAI_LIVE_API=async \
  python -m unittest tests.live.test_api -v
```

Recovery performs no new API tests. If the provider remains unavailable or deletion cannot be confirmed, inspect its UI for the `webai-live-...` marker and resolve the pending object before running the suite again. No test harness can guarantee remote deletion while a provider is unavailable.

Uploads, downloads, retries, edits, and branching are excluded from live execution because they can consume additional quota or leave provider-side artifacts without a universal reversible operation. Their behavior remains covered by offline provider tests.
