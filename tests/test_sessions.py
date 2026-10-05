"""Session placement tests."""

from spans.config import DAY_NS
from spans.generate import Generator

from .helpers import collect_spans, make_config


def test_session_traces_stay_within_one_day():
    config = make_config(days=3)
    generator = Generator(config)
    spans = collect_spans(generator)
    days_by_trace: dict[str, set[int]] = {}
    for span in spans:
        day = (int(span["startTimeUnixNano"]) - config.start_ns) // DAY_NS
        days_by_trace.setdefault(span["traceId"], set()).add(day)
    assert generator.oracle.session_traces
    for session_id, trace_ids in generator.oracle.session_traces.items():
        days = set().union(*(days_by_trace[trace_id] for trace_id in trace_ids))
        assert len(days) == 1, f"session {session_id} spans days {sorted(days)}"
