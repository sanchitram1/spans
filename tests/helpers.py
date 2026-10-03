"""Shared test helpers."""

import json
from dataclasses import replace

from spans.config import Config


def make_config(**overrides) -> Config:
    config = Config(
        number_of_spans=60,
        number_of_traces=12,
        session_trace_coverage=0.5,
        start="2026-03-18T00:00:00Z",
        days=2,
        seed=42,
        batch_spans=7,
    )
    if overrides:
        config = replace(config, **overrides)
    return config.validate()


def collect_spans(generator):
    spans = []
    for line, _ in generator:
        request = json.loads(line)
        for resource in request["resourceSpans"]:
            for scope in resource["scopeSpans"]:
                spans.extend(scope["spans"])
    return spans


def attribute(span, key):
    for entry in span["attributes"]:
        if entry["key"] == key:
            return entry["value"]
    return None
