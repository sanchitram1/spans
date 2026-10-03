"""Determinism and OTLP validity tests."""

import json

from spans.generate import Generator

from .helpers import attribute, collect_spans, make_config


def test_same_config_is_byte_identical():
    lines_a = list(Generator(make_config()))
    lines_b = list(Generator(make_config()))
    assert [line for line, _ in lines_a] == [line for line, _ in lines_b]


def test_batch_size_does_not_change_logical_spans():
    small = collect_spans(Generator(make_config(batch_spans=3)))
    large = collect_spans(Generator(make_config(batch_spans=1000)))
    assert len(small) == len(large)
    assert [json.dumps(span, sort_keys=True) for span in small] == [
        json.dumps(span, sort_keys=True) for span in large
    ]


def test_different_seed_changes_output():
    lines_a = list(Generator(make_config(seed=1)))
    lines_b = list(Generator(make_config(seed=2)))
    assert [line for line, _ in lines_a] != [line for line, _ in lines_b]


def test_every_batch_is_a_complete_otlp_request():
    for line, _ in Generator(make_config(batch_spans=5)):
        request = json.loads(line)
        assert set(request) == {"resourceSpans"}
        for resource in request["resourceSpans"]:
            resource_attributes = {
                entry["key"] for entry in resource["resource"]["attributes"]
            }
            assert {"service.name", "model_id"} <= resource_attributes
            assert len(resource["scopeSpans"]) == 1
            assert len(resource["scopeSpans"][0]["spans"]) <= 5


def test_required_attributes_on_every_span():
    spans = collect_spans(Generator(make_config()))
    assert spans
    for span in spans:
        assert attribute(span, "openinference.span.kind") is not None
        assert attribute(span, "llm.token_count.total") is not None
        assert attribute(span, "llm.cost.total") is not None
        assert attribute(span, "input.value") is not None
        assert attribute(span, "input.mime_type") == {"stringValue": "text/plain"}
        assert attribute(span, "output.value") is not None
        assert attribute(span, "output.mime_type") == {"stringValue": "text/plain"}


def test_timestamps_within_requested_window():
    config = make_config(start="2026-03-18T00:00:00Z", days=2)
    start_ns = config.start_ns
    end_ns = start_ns + config.days * 86_400_000_000_000
    spans = collect_spans(Generator(config))
    for span in spans:
        assert start_ns <= int(span["startTimeUnixNano"]) < end_ns
        assert int(span["startTimeUnixNano"]) < int(span["endTimeUnixNano"])


def test_text_payload_format():
    spans = collect_spans(Generator(make_config()))
    for span in spans:
        input_value = attribute(span, "input.value")["stringValue"]
        assert input_value.startswith(f"{span['spanId']}:input ")
        assert input_value.split() != [input_value.split()[0]]
