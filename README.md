# spans

a simple span data generator

## Usage

```bash
ax-spans generate [options]
```

## Options

### Configuration

- `--output-dir, -o PATH`: Write a fixture bundle containing batched OTLP
  request files, manifest.json, and the correctness oracle. Refuse to overwrite
  an incompatible existing fixture.
- When `--output-dir` is omitted, write newline-delimited OTLP request batches
  to stdout. Write diagnostics only to stderr. This mode is mainly for
  previewing or piping; persisted experiments should use an output directory.
- `--batch-spans 10_000`: Maximum number of spans in each OTLP request batch.
  Generation must stream through bounded memory rather than constructing one
  giant JSON document.
  - Every generated batch must be a complete OTLP `ExportTraceServiceRequest`
    that can be sent directly to a receiver.
- `--project PROJECT`: Project/model identifier embedded in every OTLP resource.
  This is needed to ingest fixtures into separate experimental projects.
- `--seed 42`: seed for all randomized allocation and content generation.
  Identical options and seed **must** produce byte-identical fixture data. Batch
  size and output mode must not change the logical spans produced by a given
  topology configuration and seed.

### Topology options

- `--number-of-spans 100_000`: exact total number of spans to generate
- `--number-of-traces 10_000`: exact total number of traces to generate
- `--spans-per-trace 2:20`: bounds for distributing the exact span total across
  traces. This requires that $traces \times min \leq spans \leq traces \times
  max$
- `--session-trace-coverage 0.05`: share of traces belonging to sessions. The
  resulting trace count is deterministically rounded and recorded
- `--traces-per-session`: bounds for distributing session-associated traces
  across sessions. The realized session count and distribution are recorded
- `--root-spans-per-trace 1:3`: bounds for the number of root spans within each
  trace, sampled from a uniform distribution. The realized average and
  distribution are recorded. A trace’s root count must not exceed its span
  count. Remaining spans are distributed as children among its roots.

### Time Spread

- `--start 2026-03-18T00:00:00Z`: Inclusive UTC start time for generated spans
- `--days 7`: Number of UTC days across which span timestamps are distributed.
  All spans must start within `[start, start + days)`

### Attributes

- `--llm-fraction`: Share of spans assigned `openinference.span.kind=LLM`.
  Remaining spans are `CHAIN`.
- `--session-id-missing-fraction 0.25`: Share of spans within session-associated
  traces that omit `session.id`. Omissions may affect root or child spans and
  are assigned deterministically from the seed. At least one span in every
  session-associated trace must retain `session.id` so the association remains
  observable.

With `--output-dir/-o PATH` it writes a fixture bundle: batched
`ExportTraceServiceRequest` files, `manifest.json`, and `oracle.json` (the
correctness oracle). Without it, newline-delimited OTLP request batches are
streamed to stdout and diagnostics go to stderr. Identical options and seed
always produce byte-identical fixtures. Batch size and output mode never change
the logical spans produced.

Run `ax-spans generate --help` for the full option reference (topology, time
spread, and attribute controls).
