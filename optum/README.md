# Optum fanout fixture

Generated with ax-spans' multi-session generator from `../.worktrees/optum/src`, then composed into an ingestible bundle. Project name: `optum`. The data bundle is in `../fixtures/optum/`; scripts and their input profile remain here. No ingestion has run.

| Property | Value |
|---|---:|
| Window | 2026-11-06T00:00:00Z / 2026-11-08T00:00:00Z |
| Span rows / unique span IDs | 75,178 / 75,178 |
| Trace IDs, reused across both days | 58 |
| Session IDs | 1,740 |
| Root rows | 1,738 |
| Session-tagged rows | 12,132 |
| Session-tagged roots | 0 |
| Largest trace | 53,442 spans, 1,246 sessions |
| OTLP batches | 76, maximum 1,000 spans each |
| OTLP bytes / parquet bytes | 123,418,140 / 2,642,677 |

`profile.json` contains the Nov 4 sampled per-trace span/root/tagged counts without customer identifiers. The two days repeat that profile with synthetic IDs, new sessions each day, and the same trace ID per profile entry. The largest trace's exact observed 623 sessions/day is preserved; for the remaining profile, synthetic session count is roots/day (minimum one), not a claim about each customer's exact session count. Repeated customer span IDs are deliberately not reproduced. Span names, kinds, payloads and parent trees are synthetic. Child-only tagging matches sampled counts; timestamps are spread across each day. This is a topology stress fixture, not a replay.

## Count the join output

Capture the native topN selection query actually emitted by MCM/GetSessions. Then:

```sh
uv run optum/prepare_join_probes.py selection.json optum/join-probes
```

Execute the prepared queries only on the synthetic namespace datasource:

- `right-input.json`: the exact capped RHS scan. Count its returned events to measure the materialized RHS row count.
- `selection-with-counts.json`: original topN with an additional `__joined_rows` count aggregator. This keeps the selection shape and shows rows accumulated for each returned session. Summing these counts covers only returned sessions, not all sessions.
- `joined-rows-total.json`: timeseries count over the same join datasource, filters, virtual columns and intervals. Returns the total joined rows reaching aggregation across all sessions.
- `joined-rows-by-trace.json`: the same count grouped by left trace ID, locating the dominant fanout key.

The diagnostic copies disable cache and set a 60s timeout. A timeout is not a zero count. The topN extra aggregator and timeseries/groupBy variants are separate instrumented executions, not existing counters from the original MCM run. Timeseries/groupBy may choose different execution paths. For physical cursor visits in the unchanged original query, add a query-ID-scoped counter at the join cursor or use an existing matching counter if the deployed build provides one; no such instrumentation was added here.

Druid buffers the RHS and streams joined rows into aggregation. Joined row count is not the size of a materialized output table or an estimate of heap bytes. Measuring heap/RHS hash-index size requires separate memory instrumentation. Segment row counts and final result count do not measure join output.

For a complete two-day self-join, the fixture predicts:

- All left rows: sum(spans_per_trace²) = **3,064,325,716** logical pairs.
- Session-tagged left rows: sum(tagged_per_trace × spans_per_trace) = **487,464,756** logical pairs.
- One-day equivalents: **766,081,429** and **121,866,189**.

These are fixture-derived predictions, not measured Druid results. The two-day RHS is below the 100k cap. Actual emitted predicates, scan cap, time expansion and join optimizations still determine execution. The no-root-session-tag topology also means root-only sort aggregators may have no contributing rows; counting with an unfiltered count aggregator still measures rows reaching aggregation. Query correctness is out of scope.

## Reproduce the data

From `~/workspace/spans`, use a fresh output directory:

```sh
PYTHONPATH=.worktrees/optum/src .venv/bin/python optum/build.py /path/to/new/optum
```

`build.py` uses ax-spans `Config`, `Generator`, OTLP serializer and ingestion preflight. It composes the skewed profile and rewrites trace IDs, session-tag placement and timestamps before generating final checksums/parquet. It makes no network calls. `../fixtures/optum/manifest.json` records each component's generator configuration; `../fixtures/optum/topology.json` records realized counts. No query correctness oracle is produced.
