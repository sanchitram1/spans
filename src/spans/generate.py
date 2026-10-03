"""Deterministic streaming span fixture generation."""

import random
from collections.abc import Iterator
from typing import Any

from . import otlp
from .config import DAY_NS, Config
from .derive import hex_id, rng
from .oracle import Oracle

ROOT_WINDOW_NS = 3_600_000_000_000
ROOT_MAX_DURATION_NS = 5_000_000_000
SPAN_MIN_DURATION_NS = 1_000_000


def _spread(
    total: int, buckets: int, low: int, high: int, generator: random.Random
) -> list[int]:
    """Distribute exactly `total` across `buckets`, each within [low, high]."""
    counts = [low] * buckets
    remaining = total - buckets * low
    while remaining > 0:
        index = generator.randrange(buckets)
        if counts[index] < high:
            counts[index] += 1
            remaining -= 1
    return counts


def _histogram(values: list[int]) -> dict[int, int]:
    result: dict[int, int] = {}
    for value in values:
        result[value] = result.get(value, 0) + 1
    return dict(sorted(result.items()))


def _session_bucket_count(total: int, low: int, high: int) -> int:
    """Pick the session count closest to balanced within the feasible range."""
    ideal = round(total / ((low + high) / 2))
    minimum = -(-total // high)
    maximum = total // low
    return min(max(ideal, minimum), maximum)


class Plan:
    """Precomputed per-trace topology, streamed trace by trace."""

    def __init__(self, config: Config) -> None:
        self.config = config
        generator = rng(config.namespace, "plan")
        session_trace_count = config.session_trace_count
        traces = range(config.number_of_traces)
        if session_trace_count:
            chosen = sorted(generator.sample(traces, session_trace_count))
        else:
            chosen = []
        self.session_traces = set(chosen)
        low = config.min_traces_per_session
        high = config.max_traces_per_session
        if chosen:
            self.session_count = _session_bucket_count(session_trace_count, low, high)
            self.trace_counts_per_session = _spread(
                session_trace_count, self.session_count, low, high, generator
            )
        else:
            self.session_count = 0
            self.trace_counts_per_session = []
        self.session_of_trace: dict[int, int] = {}
        cursor = 0
        for session_index, count in enumerate(self.trace_counts_per_session):
            for trace_index in chosen[cursor : cursor + count]:
                self.session_of_trace[trace_index] = session_index
            cursor += count
        self.spans_per_trace = _spread(
            config.number_of_spans,
            config.number_of_traces,
            config.min_spans_per_trace,
            config.max_spans_per_trace,
            generator,
        )
        self.roots_per_trace = [
            generator.randint(
                min(config.min_roots_per_trace, count),
                min(config.max_roots_per_trace, count),
            )
            for count in self.spans_per_trace
        ]

    def realized(self) -> dict:
        spans = self.spans_per_trace
        roots = self.roots_per_trace
        return {
            "session_trace_count": len(self.session_traces),
            "session_count": self.session_count,
            "traces_per_session_histogram": _histogram(self.trace_counts_per_session),
            "spans_per_trace": {
                "mean": sum(spans) / len(spans),
                "histogram": _histogram(spans),
            },
            "roots_per_trace": {
                "mean": sum(roots) / len(roots),
                "histogram": _histogram(roots),
            },
        }


class Generator:
    """Streams OTLP request lines; accumulates the oracle as it goes."""

    def __init__(self, config: Config) -> None:
        self.config = config.validate()
        self.oracle = Oracle()
        self.plan = Plan(self.config)

    def __iter__(self) -> Iterator[tuple[str, int]]:
        config = self.config
        buffer: list[dict[str, Any]] = []
        for span in self._spans():
            buffer.append(span)
            if len(buffer) == config.batch_spans:
                yield otlp.request_line(buffer, config.project), len(buffer)
                buffer = []
        if buffer:
            yield otlp.request_line(buffer, config.project), len(buffer)

    def _spans(self) -> Iterator[dict[str, Any]]:
        for trace_index in range(self.config.number_of_traces):
            yield from self._trace_spans(trace_index)
        self.oracle.traces = self.config.number_of_traces
        self.oracle.sessions = self.plan.session_count

    def _trace_spans(self, trace_index: int) -> Iterator[dict[str, Any]]:
        config = self.config
        generator = rng(config.namespace, "trace", trace_index)
        trace_id = hex_id(config.namespace, "trace", trace_index, length=32)
        span_count = self.plan.spans_per_trace[trace_index]
        root_count = self.plan.roots_per_trace[trace_index]
        session_id = self._session_id(trace_index)
        if session_id is not None:
            self.oracle.session_traces.setdefault(session_id, []).append(trace_id)
            keep_index = rng(config.namespace, "session-keep", trace_index).randrange(
                span_count
            )
        else:
            keep_index = None
        trace_start = config.start_ns + generator.randrange(config.days * DAY_NS)
        window_end = config.start_ns + config.days * DAY_NS
        roots: list[tuple[int, int]] = []
        for position in range(span_count):
            span_id = hex_id(config.namespace, "span", trace_index, position, length=16)
            if position < root_count:
                start, end = self._root_window(
                    generator, position, trace_start, window_end
                )
                roots.append((start, end - start))
                parent_id = None
            else:
                parent_position = generator.randrange(root_count)
                parent_start, parent_duration = roots[parent_position]
                offset = generator.randrange(parent_duration)
                duration = generator.randint(1, parent_duration - offset)
                start, end = parent_start + offset, parent_start + offset + duration
                parent_id = hex_id(
                    config.namespace, "span", trace_index, parent_position, length=16
                )
            if session_id is None or position == keep_index:
                span_session_id = session_id
            elif (
                rng(config.namespace, "session-drop", trace_id, span_id).random()
                < config.session_id_missing_fraction
            ):
                span_session_id = None
            else:
                span_session_id = session_id
            yield self._span(
                trace_id, span_id, parent_id, start, end, span_session_id, session_id
            )

    def _root_window(
        self,
        generator: random.Random,
        root_index: int,
        trace_start: int,
        window_end: int,
    ) -> tuple[int, int]:
        if root_index:
            bound = min(ROOT_WINDOW_NS, window_end - trace_start)
            start = trace_start + generator.randrange(bound)
        else:
            start = trace_start
        maximum = min(ROOT_MAX_DURATION_NS, window_end - start)
        duration = generator.randint(
            SPAN_MIN_DURATION_NS, max(SPAN_MIN_DURATION_NS, maximum)
        )
        return start, start + duration

    def _session_id(self, trace_index: int) -> str | None:
        session_index = self.plan.session_of_trace.get(trace_index)
        if session_index is None:
            return None
        return hex_id(self.config.namespace, "session", session_index, length=32)

    def _span(
        self,
        trace_id: str,
        span_id: str,
        parent_id: str | None,
        start_ns: int,
        end_ns: int,
        session_id: str | None,
        trace_session_id: str | None,
    ) -> dict[str, Any]:
        config = self.config
        oracle = self.oracle
        if trace_session_id is None:
            oracle.sessionless_trace_spans += 1
        elif session_id is not None:
            oracle.spans_with_session_id += 1
        else:
            oracle.spans_without_session_id += 1
        kind_rng = rng(config.namespace, "kind", trace_id, span_id)
        kind = "LLM" if kind_rng.random() < config.llm_fraction else "CHAIN"
        input_value = otlp.text_value(trace_id, span_id, "input")
        output_value = otlp.text_value(trace_id, span_id, "output")
        oracle.text_bytes += len(input_value) + len(output_value)
        token_rng = rng(config.namespace, "tokens", trace_id, span_id)
        prompt = token_rng.randint(1, 2048)
        completion = token_rng.randint(0, 1024)
        oracle.prompt_tokens += prompt
        oracle.completion_tokens += completion
        oracle.prompt_cost_micros += prompt * 3
        oracle.completion_cost_micros += completion * 15
        model = None
        if kind == "LLM":
            model = kind_rng.choice(otlp.MODEL_NAMES)
            oracle.llm_spans += 1
        else:
            oracle.chain_spans += 1
        oracle.spans += 1
        return otlp.span(
            trace_id=trace_id,
            span_id=span_id,
            parent_id=parent_id,
            kind=kind,
            start_ns=start_ns,
            end_ns=end_ns,
            session_id=session_id,
            project=config.project,
            prompt_tokens=prompt,
            completion_tokens=completion,
            input_value=input_value,
            output_value=output_value,
            model=model,
        )
