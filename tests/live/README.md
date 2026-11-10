# Optional live provider API checks

These checks use real web sessions. They are excluded from the normal `test_*.py` discovery and from CI. Importing the module and running it without `--execute` never reads credentials or opens a network connection.

## Credentials

The ignored local file `.config/credentials.json` is created with an empty account list. Fill it locally; never commit it or paste its contents into chat. The file must be owned by your user with mode `0600`, and `.config/` must be mode `0700` on POSIX systems. The runner refuses symlinks, permissive permissions, malformed data, or a credential path that Git does not ignore. The JSON is plaintext at rest; filesystem permissions are not encryption.

Example shape (replace the placeholder locally with the exact credential payload accepted by the account/provider):

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

`id` is a private local alias. The runner uses only the provider and alias you explicitly select; it never searches browser profiles or tries other accounts.

## Run modes

From the repository root:

```sh
python -m tests.live
python -m tests.live --execute --provider chatgpt --account personal
python -m tests.live --execute --provider chatgpt --account personal --api sync
python -m tests.live --execute --provider chatgpt --account personal --allow-temporary-writes
python -m tests.live --execute --provider chatgpt --account personal --allow-temporary-writes --allow-one-generation
```

The first command is a no-network plan. A live run always selects one provider, one account, and one API style (`async` by default; `sync` is available separately). Run each mode in a separate invocation if both public APIs need verification. Read-only mode opens one session and checks the model catalog plus chat/project metadata supported by that account; it does not open existing conversations or read message content. The project-write mode creates one uniquely marked project, edits only that project, adds/removes one short text resource, then deletes and verifies the project. The optional generation mode sends exactly one low-effort prompt in a uniquely marked temporary chat, checks its stream/history, and deletes and verifies that chat. It never calls `provider.ask()`, whose convenience result does not expose the temporary chat handle for cleanup.

Public API actions are serial, separated by a fixed one-second cooldown, bounded to 20 actions, and never retried automatically. Provider implementations may make multiple internal HTTP requests for a single public action; these cannot all be paced by this harness. Async calls have a 90-second action timeout. Sync calls use their provider transport's configured timeout. A rate-limit, authentication, network, or provider error stops the run. Log output includes only action names, status, and duration; response text, prompt text, cookies, tokens, account IDs, and remote IDs are suppressed.

Before each remote create, the runner writes a private cleanup journal. It removes only remote IDs returned by the test-created resource or a unique test marker found during recovery. Deletion is verified with a follow-up lookup. If the provider or network prevents verification, the run stops, preserves the journal, and refuses further tests until recovery:

```sh
python -m tests.live --execute --provider chatgpt --account personal --recover
```

No client can guarantee deletion while a provider is unavailable. In that case the test deliberately fails closed; inspect the provider UI for the unique `webai-live-...` marker, then retry recovery. Never delete a resource unless its test marker/record confirms it was created by this runner. If a stale `.config/live-run.lock` remains after a forced process kill, verify no runner is active before removing that lock and running recovery.

Uploads/downloads, retries, branches, and additional generations are intentionally not run live: they can leave provider-side files/branches or consume extra quota without a universal reversible operation. Their deterministic behavior stays in the offline test suite. The live suite checks the common, safely reversible API path and account-specific capabilities; it does not claim every model or every provider-internal operation was exercised.
