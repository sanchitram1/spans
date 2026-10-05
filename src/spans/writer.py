"""Fixture bundle writing: manifest, oracle, checksums, and idempotent reuse."""

import hashlib
import json
import shutil
import tempfile
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .config import GENERATOR_VERSION, Config
from .generate import Generator


def write(config: Config, output: Path) -> tuple[dict, dict]:
    """Write the fixture bundle to `output` and return manifest and oracle.

    Idempotent: if `output` already holds a complete fixture whose config and
    checksums verify, it is returned untouched. Any other nonempty directory
    is rejected, never deleted. Fresh output is staged in a sibling temporary
    directory and renamed into place atomically.
    """
    config.validate()
    output = Path(output)
    verified = _verified_bundle(output, config)
    if verified is not None:
        return verified
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    try:
        manifest, oracle_document = _write_bundle(config, staging)
        if output.exists():
            output.rmdir()  # only reachable when empty; verified above
        staging.rename(output)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return manifest, oracle_document


def _verified_bundle(output: Path, config: Config) -> tuple[dict, dict] | None:
    """Return the existing manifest and oracle if `output` holds this fixture.

    Returns None when the directory is absent or empty (safe to generate);
    raises ValueError otherwise, describing why the directory is refused.
    """
    if not output.exists():
        return None
    if not output.is_dir():
        raise ValueError(f"{output} exists and is not a directory")
    entries = {path.name for path in output.iterdir()}
    if not entries:
        return None
    for required in ("manifest.json", "oracle.json"):
        if required not in entries:
            raise ValueError(
                f"{output} is nonempty but has no {required}; refusing to overwrite"
            )
    try:
        manifest = json.loads((output / "manifest.json").read_text())
        oracle_document = json.loads((output / "oracle.json").read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"{output} holds an unreadable fixture: {exc}") from None
    if (
        manifest.get("generator_version") != GENERATOR_VERSION
        or manifest.get("config") != config.content()
        or manifest.get("batch_spans") != config.batch_spans
    ):
        raise ValueError(
            f"{output} holds a fixture from a different configuration; "
            "refusing to overwrite"
        )
    expected = {entry["path"] for entry in manifest.get("files", [])} | {
        "manifest.json",
        "oracle.json",
        "spans.parquet",
    }
    if entries != expected:
        raise ValueError(
            f"{output} is an incomplete fixture: expected exactly {sorted(expected)}, "
            f"found {sorted(entries)}; remove the directory to regenerate"
        )
    for entry in manifest["files"]:
        payload = (output / entry["path"]).read_bytes()
        if (
            len(payload) != entry["bytes"]
            or hashlib.sha256(payload).hexdigest() != entry["sha256"]
        ):
            raise ValueError(
                f"{output}/{entry['path']} fails checksum; fixture is corrupt, "
                "remove the directory to regenerate"
            )
    if "parquet" in manifest:
        entry = manifest["parquet"]
        payload = (output / entry["path"]).read_bytes()
        if (
            len(payload) != entry["bytes"]
            or hashlib.sha256(payload).hexdigest() != entry["sha256"]
        ):
            raise ValueError(
                f"{output}/{entry['path']} fails checksum; fixture is corrupt, "
                "remove the directory to regenerate"
            )
    return manifest, oracle_document


def _write_bundle(config: Config, staging: Path) -> tuple[dict, dict]:
    generator = Generator(config)
    files: list[dict[str, Any]] = []
    for index, (line, span_count) in enumerate(generator):
        payload = line.encode()
        path = staging / f"traces-{index:06d}.jsonl"
        path.write_bytes(payload)
        files.append(
            {
                "path": path.name,
                "spans": span_count,
                "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )

    # Write the parquet span table
    schema = pa.schema(
        [
            ("span_id", pa.string()),
            ("trace_id", pa.string()),
            ("session_id", pa.string()),
            ("span_kind", pa.string()),
            ("start_time", pa.timestamp("ns", tz="UTC")),
            ("total_tokens", pa.int64()),
            ("total_cost", pa.float64()),
            ("parent_span_id", pa.string()),
            ("span_session_id", pa.string()),
        ]
    )
    table = pa.Table.from_pydict(generator.parquet_data, schema=schema)
    parquet_path = staging / "spans.parquet"
    pq.write_table(table, parquet_path)

    parquet_payload = parquet_path.read_bytes()

    manifest = {
        "generator_version": GENERATOR_VERSION,
        "seed": config.seed,
        "config": config.content(),
        "batch_spans": config.batch_spans,
        "files": files,
        "total_bytes": sum(entry["bytes"] for entry in files),
        "parquet": {
            "path": "spans.parquet",
            "bytes": len(parquet_payload),
            "sha256": hashlib.sha256(parquet_payload).hexdigest(),
        },
    }
    oracle_document = {
        "counts": generator.oracle.summary(),
        "realized": generator.plan.realized(),
        "relationships": {"session_traces": generator.oracle.session_traces},
    }
    (staging / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    (staging / "oracle.json").write_text(
        json.dumps(oracle_document, indent=2, sort_keys=True) + "\n"
    )
    return manifest, oracle_document
