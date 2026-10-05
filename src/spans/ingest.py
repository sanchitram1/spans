"""Ingestion of fixture bundles over OTLP/HTTP JSON.

Ingestion is resumable: an atomic sidecar checkpoint records the last batch
the receiver fully accepted, and a later invocation continues from the next
batch. A batch counts as accepted only when the receiver returned a success
status and reported zero rejected spans.
"""

import hashlib
import http.client
import json
import os
import random
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from functools import partial
from pathlib import Path
from typing import TextIO
from urllib.parse import urlparse

from .config import GENERATOR_VERSION

CHECKPOINT_NAME = ".ax-spans-ingest.json"
ENDPOINT_ENV = "AX_SPANS_OTLP_HTTP_ENDPOINT"
SPACE_KEY_ENV = "AX_SPANS_SPACE_KEY"
API_KEY_ENV = "AX_SPANS_API_KEY"

RETRYABLE_STATUS = frozenset({429, 502, 503, 504})
_SUCCESS_STATUSES = frozenset(range(200, 300))
BACKOFF_BASE = 0.5
BACKOFF_CAP = 30.0


class IngestError(Exception):
    """Ingestion failed; the checkpoint still reflects the last confirmed batch."""


class RejectedSpansError(IngestError):
    """The receiver accepted the request but rejected spans; never retry."""


class SendError(IngestError):
    """A request did not complete safely; the checkpoint is unchanged."""


@dataclass(frozen=True)
class Batch:
    path: str
    spans: int
    size: int


@dataclass(frozen=True)
class Fixture:
    directory: Path
    project: str
    batches: tuple[Batch, ...]
    checksum: str

    @property
    def total_spans(self) -> int:
        return sum(batch.spans for batch in self.batches)

    @property
    def total_bytes(self) -> int:
        return sum(batch.size for batch in self.batches)


@dataclass(frozen=True)
class Checkpoint:
    manifest_sha256: str
    endpoint: str
    next_batch: int
    next_line: int
    confirmed_spans: int
    confirmed_bytes: int

    def document(self) -> dict:
        return {
            "manifest_sha256": self.manifest_sha256,
            "endpoint": self.endpoint,
            "next_batch": self.next_batch,
            "next_line": self.next_line,
            "confirmed_spans": self.confirmed_spans,
            "confirmed_bytes": self.confirmed_bytes,
        }


def load_fixture(directory: Path) -> Fixture:
    """Validate a fixture bundle fully before anything is sent."""
    directory = Path(directory)
    try:
        payload = (directory / "manifest.json").read_bytes()
    except OSError:
        raise ValueError(f"{directory} holds no readable manifest.json") from None
    try:
        manifest = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"{directory}/manifest.json is not valid JSON: {exc}"
        ) from None
    if manifest.get("generator_version") != GENERATOR_VERSION:
        raise ValueError(
            f"manifest generator_version is {manifest.get('generator_version')!r}, "
            f"expected {GENERATOR_VERSION}"
        )
    entries = manifest.get("files")
    if not isinstance(entries, list) or not entries:
        raise ValueError("manifest declares no batch files")
    batches = tuple(_load_batch(directory, entry) for entry in entries)
    declared = {batch.path for batch in batches}
    if len(declared) != len(batches):
        raise ValueError("manifest declares duplicate batch files")
    undeclared = sorted(
        path.name for path in directory.glob("*.jsonl") if path.name not in declared
    )
    if undeclared:
        raise ValueError(
            f"fixture holds undeclared batch files: {', '.join(undeclared)}"
        )
    total_spans = sum(batch.spans for batch in batches)
    oracle_spans = _oracle_span_count(directory)
    if oracle_spans is not None and oracle_spans != total_spans:
        raise ValueError(
            f"manifest declares {total_spans} spans but oracle.json counts "
            f"{oracle_spans}; the fixture is inconsistent"
        )
    return Fixture(
        directory=directory,
        project=manifest.get("config", {}).get("project", "unknown"),
        batches=batches,
        checksum=hashlib.sha256(payload).hexdigest(),
    )


def _load_batch(directory: Path, entry: object) -> Batch:
    if not isinstance(entry, dict):
        raise ValueError("manifest file entry is not an object")
    path = entry.get("path")
    spans = entry.get("spans")
    size = entry.get("bytes")
    sha256 = entry.get("sha256")
    if not isinstance(path, str) or not path or Path(path).name != path:
        raise ValueError(f"manifest declares unsafe batch path {path!r}")
    if not isinstance(spans, int) or spans < 1:
        raise ValueError(f"manifest declares an invalid span count for {path!r}")
    if not isinstance(size, int) or size < 1:
        raise ValueError(f"manifest declares an invalid byte size for {path!r}")
    if not isinstance(sha256, str):
        raise ValueError(f"manifest declares an invalid checksum for {path!r}")
    try:
        payload = (directory / path).read_bytes()
    except OSError:
        raise ValueError(f"manifest declares {path!r} but it is missing") from None
    if len(payload) != size:
        raise ValueError(
            f"{path} is corrupt: {len(payload)} bytes, manifest declares {size}"
        )
    if hashlib.sha256(payload).hexdigest() != sha256:
        raise ValueError(f"{path} fails its manifest SHA-256 checksum")
    return Batch(path=path, spans=spans, size=size)


def _oracle_span_count(directory: Path) -> int | None:
    try:
        oracle = json.loads((directory / "oracle.json").read_text())
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError):
        return None
    counts = oracle.get("counts")
    if isinstance(counts, dict) and isinstance(counts.get("spans"), int):
        return counts["spans"]
    return None


def load_checkpoint(path: Path, fixture: Fixture, endpoint: str) -> Checkpoint | None:
    """Load the sidecar checkpoint, refusing any that does not match this run."""
    try:
        document = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError):
        raise ValueError(
            f"{path} is unreadable; remove it to restart ingestion"
        ) from None
    if document.get("manifest_sha256") != fixture.checksum:
        raise ValueError(f"{path} belongs to a different fixture; remove it to restart")
    if document.get("endpoint") != endpoint:
        raise ValueError(
            f"{path} belongs to a different endpoint; remove it to restart"
        )
    counts = tuple(document.get(key) for key in _COUNT_KEYS)
    if not all(isinstance(value, int) and value >= 0 for value in counts):
        raise ValueError(f"{path} is malformed; remove it to restart ingestion")
    next_batch, next_line, confirmed_spans, confirmed_bytes = counts
    if next_batch > len(fixture.batches):
        raise ValueError(f"{path} is malformed; remove it to restart ingestion")
    return Checkpoint(
        fixture.checksum,
        endpoint,
        next_batch,
        next_line,
        confirmed_spans,
        confirmed_bytes,
    )


_COUNT_KEYS = ("next_batch", "next_line", "confirmed_spans", "confirmed_bytes")


def save_checkpoint(path: Path, checkpoint: Checkpoint) -> None:
    """Atomically record the checkpoint after a batch is fully accepted."""
    handle, staging = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(handle, "w") as stream:
            json.dump(checkpoint.document(), stream, indent=2, sort_keys=True)
            stream.write("\n")
        Path(staging).replace(path)
    except BaseException:
        Path(staging).unlink(missing_ok=True)
        raise


class _RetryableError(Exception):
    def __init__(self, reason: str, retry_after: float | None = None):
        super().__init__(reason)
        self.retry_after = retry_after


class _AmbiguousError(Exception):
    """The request may have reached the receiver but the outcome is unknown."""


class Transport:
    """Sequential OTLP/HTTP JSON transport with safe-failure retries.

    Only failures that occur before anything was sent (connection refusal,
    connect timeout) retry automatically. Ambiguous failures where the request
    may have been accepted but the response was lost surface as `SendError`
    so the checkpoint can preserve the resume position.
    """

    def __init__(
        self,
        endpoint: str,
        space_key: str,
        api_key: str,
        *,
        timeout: float,
        max_retries: int,
        on_retry: Callable[[int, int, float, str], None] | None = None,
        connection_factory: Callable[[], http.client.HTTPConnection] | None = None,
    ) -> None:
        url = urlparse(endpoint)
        if url.scheme not in ("http", "https") or not url.hostname or not url.path:
            raise ValueError(
                "endpoint must be a complete OTLP/HTTP URL, "
                "e.g. http://localhost:4318/v1/traces"
            )
        connection = (
            http.client.HTTPSConnection
            if url.scheme == "https"
            else http.client.HTTPConnection
        )
        self._factory = connection_factory or partial(
            connection, url.hostname, url.port, timeout=timeout
        )
        self._path = url.path + (f"?{url.query}" if url.query else "")
        self._headers = {
            "Content-Type": "application/json",
            "space_key": space_key,
            "api_key": api_key,
        }
        self.max_retries = max_retries
        self._on_retry = on_retry

    def send(self, payload: bytes) -> int:
        """Send one ExportTraceServiceRequest and return the retries consumed."""
        retries = 0
        while True:
            try:
                self._exchange(payload)
                return retries
            except _RetryableError as failure:
                if retries >= self.max_retries:
                    raise SendError(
                        f"receiver unreachable after {retries} retries: {failure}"
                    ) from None
                delay = failure.retry_after
                if delay is None:
                    ceiling = min(BACKOFF_CAP, BACKOFF_BASE * 2**retries)
                    delay = random.uniform(ceiling / 2, ceiling)
                if self._on_retry is not None:
                    self._on_retry(
                        retries + 1, self.max_retries + 1, delay, str(failure)
                    )
                time.sleep(delay)
                retries += 1
            except _AmbiguousError as failure:
                raise SendError(
                    "request failed ambiguously; the receiver may hold spans whose "
                    f"response was lost: {failure}"
                ) from None

    def _exchange(self, payload: bytes) -> None:
        connection = self._factory()
        try:
            try:
                connection.connect()
            except OSError as exc:
                raise _RetryableError(_reason(exc)) from None
            try:
                connection.request(
                    "POST", self._path, body=payload, headers=self._headers
                )
                response = connection.getresponse()
                body = response.read()
            except OSError as exc:
                raise _AmbiguousError(_reason(exc)) from None
        finally:
            connection.close()
        self._verify(response.status, body, response)

    def _verify(
        self, status: int, body: bytes, response: http.client.HTTPResponse
    ) -> None:
        if status in RETRYABLE_STATUS:
            raise _RetryableError(
                f"receiver returned HTTP {status}", _retry_after(response)
            )
        if status not in _SUCCESS_STATUSES:
            print(json.loads(body) if body.strip() else {})
            raise SendError(f"receiver returned HTTP {status}")
        try:
            document = json.loads(body) if body.strip() else {}
        except json.JSONDecodeError:
            raise SendError("receiver returned an unreadable OTLP response") from None
        partial = document.get("partialSuccess")
        rejected = partial.get("rejectedSpans") if isinstance(partial, dict) else None
        try:
            rejected = int(rejected or 0)
        except (TypeError, ValueError):
            raise SendError("receiver returned an unreadable OTLP response") from None
        if rejected:
            raise RejectedSpansError(f"receiver rejected {rejected} spans")


def _reason(exc: BaseException) -> str:
    return str(exc) or exc.__class__.__name__


def _retry_after(response: http.client.HTTPResponse) -> float | None:
    header = response.getheader("Retry-After")
    if header is None:
        return None
    try:
        return max(float(header), 0.0)
    except ValueError:
        pass
    try:
        moment = parsedate_to_datetime(header)
    except (TypeError, ValueError):
        return None
    return max(moment.timestamp() - time.time(), 0.0)


def ingest(
    directory: Path,
    endpoint: str,
    space_key: str,
    api_key: str,
    *,
    max_retries: int = 5,
    timeout: float = 30.0,
    stream: TextIO | None = None,
) -> dict:
    """Ingest every batch in manifest order, resuming from the checkpoint.

    Returns a machine-readable summary once the receiver has accepted every
    span. Raises `IngestError` when a batch cannot be confirmed; the checkpoint
    then still names the last fully accepted batch.
    """
    directory = Path(directory)
    stream = stream if stream is not None else sys.stderr
    fixture = load_fixture(directory)
    checkpoint_path = directory / CHECKPOINT_NAME
    saved = load_checkpoint(checkpoint_path, fixture, endpoint)
    state = saved or Checkpoint(fixture.checksum, endpoint, 0, 0, 0, 0)
    started = time.monotonic()
    retries = 0

    def note_retry(attempt: int, limit: int, delay: float, reason: str) -> None:
        print(
            f"ingest: retrying batch {state.next_batch + 1}/{len(fixture.batches)} "
            f"(attempt {attempt}/{limit}) in {delay:.1f}s: {reason}",
            file=stream,
        )

    transport = Transport(
        endpoint,
        space_key,
        api_key,
        timeout=timeout,
        max_retries=max_retries,
        on_retry=note_retry,
    )
    for index in range(state.next_batch, len(fixture.batches)):
        batch = fixture.batches[index]
        payloads = (directory / batch.path).read_bytes().splitlines(keepends=True)
        first_line = state.next_line if index == state.next_batch else 0
        for offset in range(first_line, len(payloads)):
            retries += transport.send(payloads[offset])
            state = Checkpoint(
                fixture.checksum,
                endpoint,
                index,
                offset + 1,
                state.confirmed_spans,
                state.confirmed_bytes,
            )
            save_checkpoint(checkpoint_path, state)
        state = Checkpoint(
            fixture.checksum,
            endpoint,
            index + 1,
            0,
            state.confirmed_spans + batch.spans,
            state.confirmed_bytes + batch.size,
        )
        save_checkpoint(checkpoint_path, state)
        elapsed = max(time.monotonic() - started, 1e-9)
        print(
            f"ingest: batch {index + 1}/{len(fixture.batches)} "
            f"spans {state.confirmed_spans}/{fixture.total_spans} "
            f"bytes {state.confirmed_bytes}/{fixture.total_bytes} "
            f"elapsed {elapsed:.1f}s "
            f"throughput {state.confirmed_bytes / elapsed / 1e6:.1f} MB/s "
            f"retries {retries}",
            file=stream,
        )
    return {
        "fixture": str(directory),
        "project": fixture.project,
        "endpoint": endpoint,
        "spans": state.confirmed_spans,
        "batches": len(fixture.batches),
        "bytes": state.confirmed_bytes,
        "retries": retries,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
