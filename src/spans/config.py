"""Content-defining configuration for span fixture generation."""

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from functools import cached_property

GENERATOR_VERSION = 1

DEFAULT_SEED = 42
DEFAULT_START = "2026-03-18T00:00:00Z"
DEFAULT_PROJECT = "ax-spans-fixture"

DAY_NS = 86_400_000_000_000
UINT64_MAX = 2**64 - 1

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def parse_instant(value: str) -> int:
    """Parse an ISO 8601 instant to nanoseconds since the Unix epoch."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    moment = datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    delta = moment - _EPOCH
    ns = (
        delta.days * 86_400 + delta.seconds
    ) * 1_000_000_000 + delta.microseconds * 1_000
    if not 0 <= ns <= UINT64_MAX:
        raise ValueError(
            f"instant {value!r} is outside the OTLP uint64 nanosecond range"
        )
    return ns


def digest(*parts: object) -> str:
    hasher = hashlib.sha256()
    for part in parts:
        hasher.update(str(part).encode())
        hasher.update(b"\x00")
    return hasher.hexdigest()


@dataclass(frozen=True)
class Config:
    """Content-defining parameters of a fixture run.

    `batch_spans` is excluded from the identity namespace so that batch size
    and output mode never change the logical spans produced.
    """

    number_of_spans: int = 100_000
    number_of_traces: int = 10_000
    min_spans_per_trace: int = 2
    max_spans_per_trace: int = 20
    session_trace_coverage: float = 0.05
    min_traces_per_session: int = 1
    max_traces_per_session: int = 4
    min_roots_per_trace: int = 1
    max_roots_per_trace: int = 3
    start: str = DEFAULT_START
    days: int = 7
    llm_fraction: float = 0.7
    session_id_missing_fraction: float = 0.25
    project: str = DEFAULT_PROJECT
    seed: int = DEFAULT_SEED
    batch_spans: int = 10_000

    def content(self) -> dict:
        return {
            key: value for key, value in asdict(self).items() if key != "batch_spans"
        }

    @cached_property
    def session_trace_count(self) -> int:
        return round(self.number_of_traces * self.session_trace_coverage)

    @cached_property
    def namespace(self) -> str:
        canonical = json.dumps(self.content(), sort_keys=True, separators=(",", ":"))
        return digest(GENERATOR_VERSION, canonical)[:16]

    @cached_property
    def start_ns(self) -> int:
        return parse_instant(self.start)

    def validate(self) -> "Config":
        if self.number_of_traces < 1:
            raise ValueError("number-of-traces must be >= 1")
        if not 1 <= self.min_spans_per_trace <= self.max_spans_per_trace:
            raise ValueError("spans-per-trace range must satisfy 1 <= MIN <= MAX")
        low = self.number_of_traces * self.min_spans_per_trace
        high = self.number_of_traces * self.max_spans_per_trace
        if not low <= self.number_of_spans <= high:
            raise ValueError(
                f"number-of-spans must satisfy {low} (traces x min) "
                f"<= {self.number_of_spans} <= {high} (traces x max)"
            )
        if not 0.0 <= self.session_trace_coverage <= 1.0:
            raise ValueError("session-trace-coverage must be within [0, 1]")
        if not 1 <= self.min_traces_per_session <= self.max_traces_per_session:
            raise ValueError("traces-per-session range must satisfy 1 <= MIN <= MAX")
        self._validate_session_feasibility()
        if not 1 <= self.min_roots_per_trace <= self.max_roots_per_trace:
            raise ValueError("roots-per-trace range must satisfy 1 <= MIN <= MAX")
        if self.days < 1:
            raise ValueError("days must be >= 1")
        for name in ("llm_fraction", "session_id_missing_fraction"):
            if not 0.0 <= getattr(self, name) <= 1.0:
                raise ValueError(f"{name} must be within [0, 1]")
        if self.batch_spans < 1:
            raise ValueError("batch-spans must be >= 1")
        if self.start_ns + self.days * DAY_NS > UINT64_MAX:
            raise ValueError("fixture window exceeds OTLP timestamp range")
        return self

    def _validate_session_feasibility(self) -> None:
        total = self.session_trace_count
        if total == 0:
            return
        minimum = -(-total // self.max_traces_per_session)
        maximum = total // self.min_traces_per_session
        if minimum > maximum:
            raise ValueError(
                f"no session count can hold {total} session traces within "
                f"{self.min_traces_per_session}:{self.max_traces_per_session}"
            )
