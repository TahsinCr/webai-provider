# Live provider API tests

This package runs bounded integration checks against real provider accounts. It is separate from the deterministic offline suite, and importing it never sends requests. `run_live_tests.py` runs the offline suite first, then tests the four providers through both synchronous and asynchronous APIs unless `WEBAI_LIVE_PROVIDER` or `WEBAI_LIVE_API` narrows the matrix.

Each feature file contains concrete `unittest.TestCase` and `unittest.IsolatedAsyncioTestCase` tests for that API area: sessions, models, chats, projects, or messages. `tests/live/support/case.py` centralizes account selection, guarded session setup, pacing, and cleanup. Run the suite through `run_live_tests.py`; it opens and closes a session for each feature case so async sessions stay on their test's event loop.

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

## Run the suites

From the repository root:

```sh
python run_tests.py
python run_live_tests.py
```

The first script excludes `tests/live` entirely. The second stops if any offline test fails, then runs live checks serially. By default it tests ChatGPT, Claude, Gemini, and Grok with both API styles, pausing one second between provider/API sessions. Limit the run when needed:

```sh
WEBAI_LIVE_PROVIDER=chatgpt WEBAI_LIVE_API=async python run_live_tests.py
```

`WEBAI_LIVE_ACCOUNT` selects the same local alias for each selected provider. If a provider has multiple saved accounts, set the alias; the suite will not guess. Without that setting, one configured account is selected automatically, or the default browser is imported when none exists.

## Run one provider and API style directly

From the repository root, explicitly select one provider account and API style. Discovery still runs each feature as its own test case:

```sh
WEBAI_LIVE_TESTS=1 WEBAI_LIVE_PROVIDER=chatgpt WEBAI_LIVE_ACCOUNT=personal WEBAI_LIVE_API=async \
  python -m unittest discover -s tests/live -t . -p 'test_*.py' -f -v
```

Use `WEBAI_LIVE_API=sync` to test the synchronous API. The test base class skips the other API style, and `-f` stops on the first failure to limit further provider calls.

## Coverage and limits

The live suite checks the public session, model, chat, project, and message flows: session opening/capabilities, model listing, chat/project metadata, temporary project create/rename/instruction update/text resource add/remove/delete, and one short message stream plus history/rename/delete in a temporary chat. Provider capabilities determine which optional project actions run. This is a safe integration smoke suite, not a live call of every public method: offline tests cover deterministic API contracts, while upload/download and additional-generation operations remain excluded to limit quota and remote side effects.

Calls are serialized with a one-second cooldown between public API actions, a 32-action limit per provider/API run, and no automatic retries. Each feature opens and closes its own session so async clients stay on their isolated test loop; session opens count toward the action limit, while closes release local resources. Providers may make multiple HTTP requests inside one API action, so the harness cannot pace every internal request. Async calls have a 90-second timeout; sync calls use the provider transport timeout. The report contains action names and durations only, never prompts, responses, credentials, or remote IDs.

Before creating a remote test object, the suite writes a private cleanup journal. It deletes only an object whose remote title contains the run's unique random marker, then verifies the deletion. If a provider or network error prevents confirmation, the suite keeps the journal and blocks another run. Recover only the selected account's pending test objects with:

```sh
WEBAI_LIVE_TESTS=1 WEBAI_LIVE_RECOVER=1 WEBAI_LIVE_PROVIDER=chatgpt WEBAI_LIVE_ACCOUNT=personal WEBAI_LIVE_API=async \
  python run_live_tests.py
```

Recovery performs no new API tests. If the provider remains unavailable or deletion cannot be confirmed, inspect its UI for the `webai-live-...` marker and resolve the pending object before running the suite again. No test harness can guarantee remote deletion while a provider is unavailable.

Uploads, downloads, retries, edits, and branching are excluded from live execution because they consume additional quota or can leave provider-side artifacts without a universal reversible operation. Their behavior remains covered by offline tests; a green live run does not claim these operations were exercised against the provider.
