"""Emission-order tests: the stream is chronological and parents precede children."""

from collections import defaultdict

from spans.generate import Generator

from .helpers import collect_spans, make_config


def _spans(**overrides):
    config = make_config(number_of_spans=400, number_of_traces=40, **overrides)
    return collect_spans(Generator(config))


def test_start_times_non_decreasing_across_stream():
    starts = [int(span["startTimeUnixNano"]) for span in _spans()]
    assert starts == sorted(starts)


def test_parents_appear_before_children():
    seen = set()
    for span in _spans():
        parent = span.get("parentSpanId")
        if parent is not None:
            assert parent in seen, f"child {span['spanId']} emitted before {parent}"
        seen.add(span["spanId"])


def test_each_trace_is_emitted_in_start_order():
    per_trace = defaultdict(list)
    for span in _spans():
        per_trace[span["traceId"]].append(int(span["startTimeUnixNano"]))
    assert per_trace
    for trace_id, starts in per_trace.items():
        assert starts == sorted(starts), f"trace {trace_id} not in start order"
