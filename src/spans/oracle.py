"""Correctness oracle accumulated while generating spans."""


class Oracle:
    """Exact expected outcomes recorded while streaming spans."""

    def __init__(self) -> None:
        self.spans = 0
        self.traces = 0
        self.sessions = 0
        self.llm_spans = 0
        self.chain_spans = 0
        self.spans_with_session_id = 0
        self.spans_without_session_id = 0
        self.sessionless_trace_spans = 0
        self.prompt_tokens = 0
        self.completion_tokens = 0
        self.prompt_cost_micros = 0
        self.completion_cost_micros = 0
        self.text_bytes = 0
        self.session_traces: dict[str, list[str]] = {}

    def summary(self) -> dict:
        return {
            "spans": self.spans,
            "traces": self.traces,
            "sessions": self.sessions,
            "llm_spans": self.llm_spans,
            "chain_spans": self.chain_spans,
            "spans_with_session_id": self.spans_with_session_id,
            "spans_without_session_id": self.spans_without_session_id,
            "sessionless_trace_spans": self.sessionless_trace_spans,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.prompt_tokens + self.completion_tokens,
            "prompt_cost": self.prompt_cost_micros / 1_000_000,
            "completion_cost": self.completion_cost_micros / 1_000_000,
            "total_cost": (self.prompt_cost_micros + self.completion_cost_micros)
            / 1_000_000,
            "text_bytes": self.text_bytes,
        }
