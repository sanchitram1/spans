"""Topology correctness tests."""

from spans.generate import Generator

from .helpers import attribute, collect_spans, make_config


def test_exact_span_and_trace_counts():
    generator = Generator(make_config())
    spans = collect_spans(generator)
    assert len(spans) == 60
    assert len({span["traceId"] for span in spans}) == 12
    assert generator.oracle.spans == 60
    assert generator.oracle.traces == 12


def test_spans_per_trace_within_bounds():
    config = make_config()
    generator = Generator(config)
    counts = generator.plan.spans_per_trace
    assert sum(counts) == config.number_of_spans
    assert all(
        config.min_spans_per_trace <= count <= config.max_spans_per_trace
        for count in counts
    )


def test_root_counts_within_bounds_and_at_most_span_count():
    config = make_config()
    generator = Generator(config)
    for span_count, root_count in zip(
        generator.plan.spans_per_trace, generator.plan.roots_per_trace, strict=True
    ):
        assert config.min_roots_per_trace <= root_count <= config.max_roots_per_trace
        assert root_count <= span_count
    realized = generator.plan.realized()
    assert realized["roots_per_trace"]["mean"] > 0


def test_children_point_at_roots():
    generator = Generator(make_config(min_roots_per_trace=1, max_roots_per_trace=3))
    spans = collect_spans(generator)
    by_trace = {}
    for span in spans:
        by_trace.setdefault(span["traceId"], []).append(span)
    for trace_spans in by_trace.values():
        roots = {span["spanId"] for span in trace_spans if "parentSpanId" not in span}
        for span in trace_spans:
            if "parentSpanId" in span:
                assert span["parentSpanId"] in roots


def test_session_coverage_realized():
    config = make_config(session_trace_coverage=0.5)
    generator = Generator(config)
    collect_spans(generator)
    assert config.session_trace_count == 6
    realized = generator.plan.realized()
    assert realized["session_trace_count"] == 6
    assert realized["session_count"] > 0
    assert (
        sum(realized["traces_per_session_histogram"].values())
        == realized["session_count"]
    )
    assert (
        sum(
            count * frequency
            for count, frequency in realized["traces_per_session_histogram"].items()
        )
        == 6
    )
    assert len(generator.oracle.session_traces) == realized["session_count"]
    for traces in generator.oracle.session_traces.values():
        assert config.min_traces_per_session <= len(traces)
        assert len(traces) <= config.max_traces_per_session


def test_zero_session_coverage_has_no_sessions():
    generator = Generator(make_config(session_trace_coverage=0.0))
    collect_spans(generator)
    assert generator.oracle.sessions == 0
    assert generator.oracle.session_traces == {}
    assert generator.oracle.spans_with_session_id == 0


def test_full_session_coverage():
    config = make_config(session_trace_coverage=1.0)
    generator = Generator(config)
    collect_spans(generator)
    assert generator.oracle.sessions > 0
    assert generator.oracle.spans_without_session_id < generator.oracle.spans


def test_session_traces_retain_at_least_one_session_id():
    generator = Generator(make_config())
    spans = collect_spans(generator)
    session_traces = {
        trace_id
        for traces in generator.oracle.session_traces.values()
        for trace_id in traces
    }
    by_trace = {}
    for span in spans:
        by_trace.setdefault(span["traceId"], []).append(span)
    assert session_traces
    for trace_id in session_traces:
        retained = [
            span
            for span in by_trace[trace_id]
            if attribute(span, "session.id") is not None
        ]
        assert retained, f"trace {trace_id} lost its session.id"


def test_missing_session_id_fraction_applies_within_session_traces():
    generator = Generator(make_config())
    collect_spans(generator)
    summary = generator.oracle.summary()
    session_trace_spans = (
        summary["spans_with_session_id"] + summary["spans_without_session_id"]
    )
    assert 0 < session_trace_spans < summary["spans"]
    assert summary["spans_without_session_id"] < 0.5 * session_trace_spans
    assert (
        summary["spans_without_session_id"] + summary["sessionless_trace_spans"]
        == summary["spans"] - summary["spans_with_session_id"]
    )


def test_span_kinds_partition():
    generator = Generator(make_config(llm_fraction=0.5))
    spans = collect_spans(generator)
    kinds = {
        attribute(span, "openinference.span.kind")["stringValue"] for span in spans
    }
    assert kinds <= {"LLM", "CHAIN"}
    summary = generator.oracle.summary()
    assert summary["llm_spans"] + summary["chain_spans"] == summary["spans"]
