# Live provider API tests

This package runs bounded integration checks against real provider accounts. It is separate from the deterministic offline suite, and importing it never sends requests. `run_live_tests.py` runs the offline suite first, then tests the four providers through the asynchronous API. The synchronous API is covered by the offline suite. Limit the live run with `WEBAI_LIVE_PROVIDER`.

Each feature file contains concrete `unittest.IsolatedAsyncioTestCase` tests for that API area: sessions, models, chats, projects, or messages. `tests/live/support/case.py` centralizes account selection, guarded session setup, pacing, and cleanup. Run the suite through `run_live_tests.py`. On Python 3.11 and later, feature cases share a provider session on the same test event loop. On Python 3.10, each case opens and closes its own session on its event loop.

## Account setup

The live suite first reuses the selected provider's account in `.config/credentials.json`. If that provider has no saved account, it imports that provider's cookies from the default browser profile and saves them there only after the required authentication cookie is present. `WEBAI_LIVE_BROWSER` can select a browser explicitly. The directory must be owned by your user with mode `0700`, and the file with mode `0600` on POSIX. Symlinks, permissive files, malformed JSON, and files not ignored by Git are rejected. Cookie values are plaintext at rest; permissions are not encryption.

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

Manual credential setup is optional. Keep a local alias in `id` and use the credential shape accepted by that provider. Credentials are never printed or committed.

When opening a live-test session fails with a typed authentication error, the harness reimports cookies from the selected browser and retries once only for the `default-browser` account. It does not refresh named accounts, and network, rate-limit, or other failures do not trigger a retry. This behavior is confined to `tests/live`; normal provider and SiopeChat runtime behavior is unchanged.

## Run the suites

From the repository root:

```sh
python run_tests.py
python run_live_tests.py
```

The first script excludes `tests/live` entirely. The second stops if any offline test fails. It runs one async phase per provider. Different providers can run concurrently where the credential store supports process locking. Limit the run with `WEBAI_LIVE_PROVIDER`:

```sh
WEBAI_LIVE_PROVIDER=chatgpt python run_live_tests.py
```

`WEBAI_LIVE_ACCOUNT` selects the same local alias for each selected provider. If a provider has multiple saved accounts, set the alias; the suite will not guess. Without that setting, one configured account is selected automatically, or the default browser is imported when none exists.

## Run one provider directly

From the repository root, explicitly select one provider account. Discovery still runs each feature as its own test case:

```sh
WEBAI_LIVE_TESTS=1 WEBAI_LIVE_PROVIDER=chatgpt WEBAI_LIVE_ACCOUNT=personal \
  python -m unittest discover -s tests/live -t . -p 'test_*.py' -f -v
```

`-f` stops on the first failure to limit further provider calls.

## Coverage and limits

| Scenario | Remote behavior exercised |
| --- | --- |
| Sessions and models | Open a session, check its status, refresh the authenticated account catalog, and send a short message to selected models with a supported typed effort when available. |
| Chats and messages | List chats, stream a marked conversation, and check ordered response parts, generation and terminal events, verification when emitted, and enum-based `wait_for(after=...)`, and terminal hooks shared across sends. Prepare that conversation before its existing follow-up, read back the response, then rename and delete it. The async phase also checks incognito mode, a small message attachment, a sourced response when the model returns sources, retry, edit, and branch where supported. Chat opening with history is checked when reading back a retry. |
| Projects and resources | List, create, rename, update instructions, add/list/remove a text resource and upload/remove a file where allowed by project options, then delete the project. The async phase checks remote chat membership after a move through the project-scoped collection. |

The suite uses disposable data and the account's advertised capabilities. ChatGPT, Claude, and Gemini run a focused set of models by default. Set `WEBAI_LIVE_ALL_MODELS=1` to request the whole available catalog within the existing action budget. A rate limit can leave part of the provider's behavior unverified. Raw transport endpoints, arbitrary downloads, destructive account operations, and provider-specific combinations outside these scenarios have deterministic offline coverage where applicable. A model that returns no sources leaves source coverage partial.

The gate applies a 150 ms minimum interval after API actions on the same account and increases it after a rate limit. It caps each provider at 96 actions. Actions time out after 300 seconds. Providers may issue multiple requests within an API action. The harness can retry session opening once after a network timeout or a recognized transient HTTP status. For a typed authentication error, it can refresh a default-browser account once. A denied request, a rate limit, or a generation rejection cannot trigger a resend.

`ACTION OK` confirms the API call returned and reports elapsed milliseconds, including time waiting for pacing. `passed` counts scenarios that completed all their checks, while `partial` counts scenarios with skipped features. Rate limits and unsupported scenarios have separate totals. A phase without a completed scenario is unverified. Known HTTP statuses, workspace-selection failures, and Gemini generation rejections appear as sanitized error labels. The report contains no prompt, response, credential, or remote ID values.

Set `WEBAI_LIVE_TIMING=1` to show available timing measurements. `action_to_text` includes test pacing and ends at the first assistant text observed by the provider. `stream_to_text` starts when the stream response opens. `first_evt` can be a control event. `action_to_last_evt` marks the last recorded stream event, while `history_after` measures a separate conversation read when one is needed. These timings have different starting points and can overlap.

The first Ctrl+C stops scheduling scenarios after the active scenarios finish and get a chance to clean up. An interrupted run exits with status 130. A second Ctrl+C can leave work for recovery.

Before dispatching a remote creation request, the suite marks that attempt in a private cleanup journal. It verifies the remote object ID and the run's unique marker in its title or chat history before deleting it, then verifies the deletion. If a provider or network error prevents confirmation, the suite keeps the journal and blocks new tests for that provider. Other selected providers still run. A run with blocked providers exits with status 2 when the providers that ran pass. Recover only the selected account's pending test objects with:

```sh
WEBAI_LIVE_TESTS=1 WEBAI_LIVE_RECOVER=1 WEBAI_LIVE_PROVIDER=chatgpt WEBAI_LIVE_ACCOUNT=personal \
  python run_live_tests.py
```

Recovery performs no new API tests. If the provider remains unavailable or deletion cannot be confirmed, inspect its UI for the `webai-live-...` marker and resolve the pending object before running the suite again. No test harness can guarantee remote deletion while a provider is unavailable.
