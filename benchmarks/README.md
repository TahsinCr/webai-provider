# Offline benchmarks

The suite uses [pyperf](https://pyperf.readthedocs.io/) worker processes and
synthetic inputs. It uses no browser profile, account credentials, sockets, or
provider test service. Install the optional tools from the repository root:

```sh
python -m pip install -e ".[benchmark]"
python run_benchmarks.py --list-cases
```

| Suite | Workloads |
| --- | --- |
| `history` | Blocking and async chat opening with one or two history reads, opening the last 25 responses from a 20,000-message history, eight overlapping reads of one chat, and a fresh read of an already opened history. |
| `collections` | Listings of 100, 1,000, and 10,000 chats, a 1,000-chat listing with an event subscriber, project fanout, and 10,000 jobs on four workers. |
| `providers` | Read a synthetic 1,000-turn history through each provider, and prepare a retry for a past Gemini or Claude turn. Fixtures replace remote calls and keep actual provider parsing and selection in the timed path. |
| `transport` | 16 KiB and 8 MiB buffered reads through sync and async HTTP transports, 4,000 stream chunks through a persistent blocking portal, and split SSE and JSPB frames. |

Each value measures one full operation. Large workloads return their batch
duration. Fixture preparation, provider opening, and event loop or portal
startup are outside the reported elapsed time. The transport workloads exercise
the library above an injected client. TLS, DNS, transfer speed, server latency,
and browser behavior require separate measurements.

## Run and compare

```sh
# One value per case to check that the entire suite runs
python run_benchmarks.py --debug-single-value

# From the baseline checkout
python run_benchmarks.py --suite history --fast -o /tmp/webai-base.json

# From the candidate checkout, using the same environment and workload
python run_benchmarks.py --suite history --fast -o /tmp/webai-candidate.json
python -m pyperf compare_to /tmp/webai-base.json /tmp/webai-candidate.json --table
```

For a narrow comparison, use the same `--match` pattern on both sides, for
example `--match 'history.open.*'`. Run on the same machine, Python build,
dependency versions, and CPU affinity. Use `--rigorous` and repeat noisy cases
before drawing conclusions. If an older revision predates the suite, use the
same benchmark sources on both revisions and compare only the cases supported
by both. Each JSON records the Git revision, dirty state, checkout fingerprint,
dependency versions, workload sizes, and pyperf's Python/host metadata. The
fingerprint excludes result files.

## Memory and CPU diagnostics

```sh
python run_benchmarks.py --suite transport --match 'transport.buffer.*.8m' \
  --tracemalloc --fast -o /tmp/webai-python-allocations.json
python run_benchmarks.py --suite history --match 'history.list.fresh.*' \
  --profile /tmp/webai-history.prof --debug-single-value
```

`--tracemalloc` reports Python allocation peaks. Where supported, `--track-memory`
records process memory. These whole sample metrics may include fixture setup and
worker overhead. Run them separately from timing and compare like metrics.
`--profile` diagnoses CPU hotspots in a separate run. Use `benchmarks/results/`
or a directory outside the checkout for generated artifacts.
