"""ax-spans command line interface."""

import argparse
import json
import os
import sys
from pathlib import Path

from .config import Config
from .generate import Generator
from .ingest import (
    API_KEY_ENV,
    ENDPOINT_ENV,
    SPACE_KEY_ENV,
    IngestError,
    ingest,
)
from .writer import write


def _pair(value: str) -> tuple[int, int]:
    try:
        low, high = (int(part) for part in value.split(":", 1))
    except ValueError:
        raise argparse.ArgumentTypeError("expected MIN:MAX, e.g. 2:20") from None
    if not 1 <= low <= high:
        raise argparse.ArgumentTypeError("range must satisfy 1 <= MIN <= MAX")
    return low, high


def _fraction(value: str) -> float:
    number = float(value)
    if not 0.0 <= number <= 1.0:
        raise argparse.ArgumentTypeError("must be within [0, 1]")
    return number


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        prog="ax-spans", description="Deterministic OpenInference span fixtures"
    )
    commands = result.add_subparsers(dest="command", required=True)
    command = commands.add_parser("generate", help="generate span fixtures")
    command.add_argument(
        "--output-dir",
        "-o",
        type=Path,
        help="fixture bundle directory; omit to stream NDJSON batches to stdout",
    )
    command.add_argument(
        "--batch-spans",
        type=int,
        default=10_000,
        help="maximum spans per OTLP request batch (default: %(default)s)",
    )
    command.add_argument(
        "--project",
        default="ax-spans-fixture",
        help="project/model identifier embedded in every OTLP resource "
        "(default: %(default)s)",
    )
    command.add_argument(
        "--seed",
        type=int,
        default=42,
        help="seed for all randomized generation (default: %(default)s)",
    )
    command.add_argument(
        "--number-of-spans",
        type=int,
        default=100_000,
        help="exact total number of spans to generate (default: %(default)s)",
    )
    command.add_argument(
        "--number-of-traces",
        type=int,
        default=10_000,
        help="exact total number of traces to generate (default: %(default)s)",
    )
    command.add_argument(
        "--spans-per-trace",
        type=_pair,
        default=(2, 20),
        metavar="MIN:MAX",
        help="bounds for distributing the span total across traces "
        "(default: %(default)s)",
    )
    command.add_argument(
        "--session-trace-coverage",
        type=_fraction,
        default=0.05,
        help="share of traces belonging to sessions (default: %(default)s)",
    )
    command.add_argument(
        "--traces-per-session",
        type=_pair,
        default=(1, 4),
        metavar="MIN:MAX",
        help="bounds for distributing session traces across sessions "
        "(default: %(default)s)",
    )
    command.add_argument(
        "--root-spans-per-trace",
        type=_pair,
        default=(1, 3),
        metavar="MIN:MAX",
        help="bounds for root spans within each trace (default: %(default)s)",
    )
    command.add_argument(
        "--start",
        default="2026-03-18T00:00:00Z",
        help="inclusive UTC start instant, ISO 8601 (default: %(default)s)",
    )
    command.add_argument(
        "--days",
        type=int,
        default=7,
        help="number of UTC days span timestamps spread across (default: %(default)s)",
    )
    command.add_argument(
        "--llm-fraction",
        type=_fraction,
        default=0.7,
        help="share of spans with openinference.span.kind=LLM; the rest are CHAIN "
        "(default: %(default)s)",
    )
    command.add_argument(
        "--session-id-missing-fraction",
        type=_fraction,
        default=0.25,
        help="share of spans within session traces that omit session.id; at least "
        "one span per session trace retains it (default: %(default)s)",
    )
    command = commands.add_parser(
        "ingest", help="ingest a fixture bundle over OTLP/HTTP JSON"
    )
    command.add_argument(
        "fixture", type=Path, help="fixture directory holding manifest.json"
    )
    command.add_argument(
        "--otlp-http-endpoint",
        help=f"complete OTLP/HTTP JSON endpoint, e.g. http://localhost:4318/v1/traces "
        f"(or {ENDPOINT_ENV})",
    )
    command.add_argument(
        "--space-key",
        help=f"receiver space key header (or {SPACE_KEY_ENV})",
    )
    command.add_argument(
        "--api-key",
        help=f"receiver API key header (or {API_KEY_ENV})",
    )
    command.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="retries for safe connection failures (default: %(default)s)",
    )
    command.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="per-request connect and response timeout in seconds "
        "(default: %(default)s)",
    )
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "ingest":
            return _ingest(args)
        return _generate(args)
    except (OSError, ValueError, IngestError) as exc:
        print(f"ax-spans: {exc}", file=sys.stderr)
        return 1


def _ingest(args: argparse.Namespace) -> int:
    endpoint = _required(args.otlp_http_endpoint, ENDPOINT_ENV, "--otlp-http-endpoint")
    space_key = _required(args.space_key, SPACE_KEY_ENV, "--space-key")
    api_key = _required(args.api_key, API_KEY_ENV, "--api-key")
    summary = ingest(
        args.fixture,
        endpoint,
        space_key,
        api_key,
        max_retries=args.max_retries,
        timeout=args.timeout,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def _required(value: str | None, env: str, flag: str) -> str:
    if value is not None:
        return value
    supplied = os.environ.get(env)
    if supplied is not None:
        return supplied
    raise ValueError(f"{flag} or {env} is required")


def _generate(args: argparse.Namespace) -> int:
    spans = args.spans_per_trace
    sessions = args.traces_per_session
    roots = args.root_spans_per_trace
    config = Config(
        number_of_spans=args.number_of_spans,
        number_of_traces=args.number_of_traces,
        min_spans_per_trace=spans[0],
        max_spans_per_trace=spans[1],
        session_trace_coverage=args.session_trace_coverage,
        min_traces_per_session=sessions[0],
        max_traces_per_session=sessions[1],
        min_roots_per_trace=roots[0],
        max_roots_per_trace=roots[1],
        start=args.start,
        days=args.days,
        llm_fraction=args.llm_fraction,
        session_id_missing_fraction=args.session_id_missing_fraction,
        project=args.project,
        seed=args.seed,
        batch_spans=args.batch_spans,
    )
    if args.output_dir is None:
        return _stream(config)
    manifest, oracle_document = write(config, args.output_dir)
    summary = {
        "output": str(args.output_dir),
        "generator_version": manifest["generator_version"],
        "files": len(manifest["files"]),
        "total_bytes": manifest["total_bytes"],
        "counts": oracle_document["counts"],
        "realized": oracle_document["realized"],
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def _stream(config: Config) -> int:
    generator = Generator(config)
    for line, _ in generator:
        sys.stdout.write(line)
    diagnostics = {
        "counts": generator.oracle.summary(),
        "realized": generator.plan.realized(),
    }
    print(json.dumps(diagnostics, indent=2, sort_keys=True), file=sys.stderr)
    return 0
