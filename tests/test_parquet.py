"""Parquet column correctness: parent_span_id and span_session_id."""

from spans.generate import Generator

from .helpers import attribute, collect_spans, make_config


def _rows(config):
    generator = Generator(config)
    spans = collect_spans(generator)
    data = generator.parquet_data
    rows = [dict(zip(data.keys(), values, strict=True)) for values in zip(*data.values(), strict=True)]
    return spans, rows


def test_parent_span_id_null_iff_root_span():
    spans, rows = _rows(make_config())
    assert spans
    for span, row in zip(spans, rows, strict=True):
        assert row["span_id"] == span["spanId"]
        if "parentSpanId" in span:
            assert row["parent_span_id"] == span["parentSpanId"]
        else:
            assert row["parent_span_id"] is None


def test_parent_span_id_count_matches_root_count():
    config = make_config()
    spans, rows = _rows(config)
    root_count = sum(1 for span in spans if "parentSpanId" not in span)
    null_count = sum(1 for row in rows if row["parent_span_id"] is None)
    assert null_count == root_count
    assert null_count == sum(Generator(config).plan.roots_per_trace)


def test_span_session_id_matches_ndjson_attribute():
    spans, rows = _rows(make_config())
    for span, row in zip(spans, rows, strict=True):
        emitted = attribute(span, "session.id")
        if emitted is None:
            assert row["span_session_id"] is None
        else:
            assert row["span_session_id"] == emitted["stringValue"]


def test_dropped_span_session_id_keeps_resolved_session_id():
    generator = Generator(make_config())
    collect_spans(generator)
    data = generator.parquet_data
    dropped = [
        (session_id, span_session_id)
        for session_id, span_session_id in zip(
            data["session_id"], data["span_session_id"], strict=True
        )
        if session_id is not None and span_session_id is None
    ]
    assert dropped
    for session_id, span_session_id in dropped:
        assert session_id is not None
        assert span_session_id is None
