"""OTLP/JSON span and request serialization."""

import json
import random
from typing import Any

from .derive import rng

SESSION_ID_KEY = "session.id"
SPAN_KIND_KEY = "openinference.span.kind"

MODEL_NAMES = ("gpt-4.1-mini", "claude-sonnet-4.5", "gemini-2.5-pro")

TEXT_MIN_BYTES = 512
TEXT_MAX_BYTES = 4096
TEXT_NONCE_INTERVAL = 4
_TEXT_WORD_SKEW_EXPONENT = 1.7

_TEXT_WORDS = tuple(
    """brindle quartz vellum juniper saffron thimble cobalt ravel
    marzipan orbit cinder willow acorn lantern meadow nimbus
    pebble fern copper hazel ember ripple walnut iris
    zephyr canvas plover tangle mossy flint amber clover sable
    dapple kernel lilac mortar opal prism quiver rattan solstice
    topaz umber violet warden xylem yarrow zinnia aster beryl
    calico dahlias ecru fable garnet heather indigo jasper kelp
    loam meridian nectar ochre pectin quince russet thistle
    umbra verdant wisp xeric yonder azure basalt carmine delta
    effulgent folio glyph hearth islet kaleidoscope lichen mosaic
    nodule ovoid parallax quasar radial silt tessera umami
    vacuity waft xanthic yawning zoetic almanac bower cask
    dither elfin frolic gossamer humbug inkling jostle knurl
    lacuna meander natter ocherous purl rosette sundial tippet
    utopia vacillate whorl xenon yodel zeppelin anise balsa
    caper dahlia eider fennel guava hyacinth icicle jicama
    kumquat loquat myrrh nutmeg oregano papaya rambutan sumac
    tamarind ugli vanilla wasabi xigua yam zaatar""".split()
)


def string_attr(key: str, value: str) -> dict:
    return {"key": key, "value": {"stringValue": value}}


def integer_attr(key: str, value: int) -> dict:
    return {"key": key, "value": {"intValue": str(value)}}


def double_attr(key: str, value: float) -> dict:
    return {"key": key, "value": {"doubleValue": value}}


def text_value(trace_id: str, span_id: str, field: str) -> str:
    """Unique deterministic ASCII text: random words plus hex nonce tokens."""
    generator = rng("text", trace_id, span_id, field)
    size = generator.randint(TEXT_MIN_BYTES, TEXT_MAX_BYTES)
    parts = [f"{span_id}:{field} "]
    length = len(parts[0])
    words_since_nonce = 0
    while length < size:
        if words_since_nonce == TEXT_NONCE_INTERVAL:
            token = f"{generator.getrandbits(48):012x} "
            words_since_nonce = 0
        else:
            token = _text_word(generator) + " "
            words_since_nonce += 1
        token = token[: size - length]
        parts.append(token)
        length += len(token)
    return "".join(parts)


def _text_word(generator: random.Random) -> str:
    """Choose from a broad vocabulary with a natural long-tail frequency."""
    index = int(generator.random() ** _TEXT_WORD_SKEW_EXPONENT * len(_TEXT_WORDS))
    return _TEXT_WORDS[index]


def span(
    *,
    trace_id: str,
    span_id: str,
    parent_id: str | None,
    kind: str,
    start_ns: int,
    end_ns: int,
    session_id: str | None,
    project: str,
    prompt_tokens: int,
    completion_tokens: int,
    input_value: str,
    output_value: str,
    model: str | None,
) -> dict[str, Any]:
    attributes = [
        string_attr(SPAN_KIND_KEY, kind),
        string_attr("model_id", project),
        integer_attr("llm.token_count.prompt", prompt_tokens),
        integer_attr("llm.token_count.completion", completion_tokens),
        integer_attr("llm.token_count.total", prompt_tokens + completion_tokens),
        double_attr("llm.cost.prompt", round(prompt_tokens * 3e-6, 6)),
        double_attr("llm.cost.completion", round(completion_tokens * 1.5e-5, 6)),
        double_attr(
            "llm.cost.total",
            round(prompt_tokens * 3e-6 + completion_tokens * 1.5e-5, 6),
        ),
        string_attr("input.value", input_value),
        string_attr("input.mime_type", "text/plain"),
        string_attr("output.value", output_value),
        string_attr("output.mime_type", "text/plain"),
    ]
    if session_id is not None:
        attributes.append(string_attr(SESSION_ID_KEY, session_id))
    if model is not None:
        attributes.append(string_attr("llm.model_name", model))
    span: dict[str, Any] = {
        "traceId": trace_id,
        "spanId": span_id,
        "name": {"CHAIN": "chain.step", "LLM": "llm.completion"}[kind],
        "kind": 1,
        "startTimeUnixNano": str(start_ns),
        "endTimeUnixNano": str(end_ns),
        "attributes": attributes,
    }
    if parent_id is not None:
        span["parentSpanId"] = parent_id
    return span


def request_line(spans: list[dict[str, Any]], project: str) -> str:
    """Serialize one batch as a complete OTLP ExportTraceServiceRequest line."""
    request = {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        string_attr("service.name", "ax-spans"),
                        string_attr("model_id", project),
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {"name": "ax-spans", "version": "1"},
                        "spans": spans,
                    }
                ],
            }
        ]
    }
    return json.dumps(request, separators=(",", ":")) + "\n"
