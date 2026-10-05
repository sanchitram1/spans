"""CLI tests for ax-spans generate."""

import json

import pyarrow.parquet as pq

from spans.cli import main
from spans.config import Config

SMALL_ARGS = [
    "generate",
    "--number-of-spans",
    "40",
    "--number-of-traces",
    "8",
    "--spans-per-trace",
    "2:20",
    "--session-trace-coverage",
    "0.5",
    "--batch-spans",
    "5",
]


def read_spans(path):
    spans = []
    for batch in sorted(path.glob("traces-*.jsonl")):
        for line in batch.read_text().splitlines():
            request = json.loads(line)
            for resource in request["resourceSpans"]:
                for scope in resource["scopeSpans"]:
                    spans.extend(scope["spans"])
    return spans


def test_stdout_mode_streams_ndjson_and_diagnostics(capsys):
    code = main([*SMALL_ARGS, "--days", "1"])
    assert code == 0
    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    assert len(lines) == 8  # 40 spans / 5 per batch
    for line in lines:
        request = json.loads(line)
        assert request["resourceSpans"]
    diagnostics = json.loads(captured.err)
    assert diagnostics["counts"]["spans"] == 40
    assert diagnostics["realized"]["session_trace_count"] == 4


def test_output_dir_mode_writes_bundle(capsys, tmp_path):
    output = tmp_path / "bundle"
    code = main([*SMALL_ARGS, "--output-dir", str(output)])
    assert code == 0
    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    assert summary["output"] == str(output)
    manifest = json.loads((output / "manifest.json").read_text())
    oracle_document = json.loads((output / "oracle.json").read_text())
    assert manifest["seed"] == 42
    assert manifest["generator_version"] == 1
    assert manifest["config"]["number_of_spans"] == 40
    assert len(manifest["files"]) == 8
    assert oracle_document["counts"]["spans"] == 40
    assert len(read_spans(output)) == sum(entry["spans"] for entry in manifest["files"])

    # Check parquet file existence and content
    parquet_path = output / "spans.parquet"
    assert parquet_path.exists()

    table = pq.read_table(parquet_path)
    assert len(table) == 40
    assert table.column_names == [
        "span_id",
        "trace_id",
        "session_id",
        "span_kind",
        "start_time",
        "total_tokens",
        "total_cost",
    ]


def test_output_dir_batch_matches_stdout_spans(capsys, tmp_path):
    output = tmp_path / "bundle"
    assert main([*SMALL_ARGS, "--output-dir", str(output)]) == 0
    capsys.readouterr()  # discard the dir-mode summary
    assert main(SMALL_ARGS) == 0
    file_spans = read_spans(output)
    stdout_spans = []
    captured = capsys.readouterr()
    for line in captured.out.splitlines():
        request = json.loads(line)
        for resource in request["resourceSpans"]:
            for scope in resource["scopeSpans"]:
                stdout_spans.extend(scope["spans"])
    assert [json.dumps(span, sort_keys=True) for span in file_spans] == [
        json.dumps(span, sort_keys=True) for span in stdout_spans
    ]


def test_rerun_is_idempotent(tmp_path):
    output = tmp_path / "bundle"
    assert main([*SMALL_ARGS, "--output-dir", str(output)]) == 0
    before = (output / "manifest.json").read_bytes()
    assert main([*SMALL_ARGS, "--output-dir", str(output)]) == 0
    assert (output / "manifest.json").read_bytes() == before


def test_refuses_incompatible_fixture(tmp_path, capsys):
    output = tmp_path / "bundle"
    assert main([*SMALL_ARGS, "--output-dir", str(output)]) == 0
    before = (output / "manifest.json").read_bytes()
    code = main([*SMALL_ARGS, "--seed", "7", "--output-dir", str(output)])
    assert code == 1
    assert "refusing" in capsys.readouterr().err
    assert (output / "manifest.json").read_bytes() == before


def test_refuses_corrupt_fixture(tmp_path, capsys):
    output = tmp_path / "bundle"
    assert main([*SMALL_ARGS, "--output-dir", str(output)]) == 0
    batch = next(iter(sorted(output.glob("traces-*.jsonl"))))
    batch.write_text(batch.read_text().replace("chain", "chian"))
    code = main([*SMALL_ARGS, "--output-dir", str(output)])
    assert code == 1
    assert "checksum" in capsys.readouterr().err


def test_refuses_foreign_directory(tmp_path, capsys):
    output = tmp_path / "bundle"
    output.mkdir()
    (output / "notes.txt").write_text("hello")
    code = main([*SMALL_ARGS, "--output-dir", str(output)])
    assert code == 1
    assert "refusing" in capsys.readouterr().err


def test_rejects_infeasible_topology(tmp_path, capsys):
    code = main(
        [
            "generate",
            "--number-of-spans",
            "10",
            "--number-of-traces",
            "8",
            "--spans-per-trace",
            "2:20",
            "--output-dir",
            str(tmp_path / "bundle"),
        ]
    )
    assert code == 1
    assert "must satisfy" in capsys.readouterr().err


def test_bad_start_is_reported(capsys):
    code = main([*SMALL_ARGS, "--start", "not-a-time"])
    assert code == 1
    assert "ax-spans:" in capsys.readouterr().err


def test_config_defaults_match_spec():
    defaults = Config()
    assert defaults.number_of_spans == 100_000
    assert defaults.number_of_traces == 10_000
    assert defaults.min_spans_per_trace == 2
    assert defaults.max_spans_per_trace == 20
    assert defaults.session_trace_coverage == 0.05
    assert defaults.min_roots_per_trace == 1
    assert defaults.max_roots_per_trace == 3
    assert defaults.session_id_missing_fraction == 0.25
    assert defaults.seed == 42
    assert defaults.start == "2026-03-18T00:00:00Z"
    assert defaults.days == 7
    assert defaults.batch_spans == 10_000
