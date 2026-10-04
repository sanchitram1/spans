"""Tests for ax-spans ingest."""

import http.client
import io
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from spans.cli import main
from spans.ingest import SendError, Transport, ingest

GEN_ARGS = [
    "generate",
    "--number-of-spans",
    "12",
    "--number-of-traces",
    "6",
    "--batch-spans",
    "2",
    "--days",
    "1",
]

INGEST_ARGS = [
    "--space-key",
    "space",
    "--api-key",
    "key",
    "--max-retries",
    "3",
    "--timeout",
    "5",
]


class Receiver:
    """Minimal threaded OTLP receiver driven by an optional per-request callback."""

    def __init__(self, respond=None):
        self.respond = respond
        self.requests = []
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                receiver.requests.append({"headers": dict(self.headers), "body": body})
                status, document = (
                    receiver.respond(len(receiver.requests) - 1)
                    if receiver.respond is not None
                    else (200, {"partialSuccess": {}})
                )
                payload = json.dumps(document).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format, *args):
                pass

        class Server(ThreadingHTTPServer):
            def handle_error(self, request, client_address):
                pass  # a dropped connection is the point in some tests

        self.server = Server(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def endpoint(self):
        host, port = self.server.server_address[:2]
        return f"http://{host}:{port}/v1/traces"

    @property
    def bodies(self):
        return [request["body"] for request in self.requests]

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


def run_ingest(fixture, endpoint, *extra):
    return main(
        ["ingest", str(fixture), "--otlp-http-endpoint", endpoint, *INGEST_ARGS, *extra]
    )


def batch_lines(fixture):
    return [path.read_bytes() for path in sorted(fixture.glob("traces-*.jsonl"))]


def test_ingest_sends_every_batch_in_order_and_summarizes(tmp_path, capsys):
    bundle = tmp_path / "bundle"
    assert main([*GEN_ARGS, "--output-dir", str(bundle)]) == 0
    capsys.readouterr()
    with Receiver() as receiver:
        code = run_ingest(bundle, receiver.endpoint)
        assert code == 0
        lines = batch_lines(bundle)
        assert [body.rstrip(b"\n") for body in receiver.bodies] == [
            line.rstrip(b"\n") for line in lines
        ]
        for request, body in zip(receiver.requests, receiver.bodies, strict=True):
            assert request["headers"]["Content-Type"] == "application/json"
            assert request["headers"]["space_key"] == "space"
            assert request["headers"]["api_key"] == "key"
            assert json.loads(body)["resourceSpans"]
    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    assert summary["fixture"] == str(bundle)
    assert summary["project"] == "ax-spans-fixture"
    assert summary["endpoint"] == receiver.endpoint
    assert summary["spans"] == 12
    assert summary["batches"] == 6
    assert summary["bytes"] == sum(len(line) for line in lines)
    assert summary["retries"] == 0
    assert summary["elapsed_seconds"] >= 0
    progress = captured.err.splitlines()
    assert len(progress) == 6
    assert "batch 1/6" in progress[0]
    assert "spans 12/12" in progress[-1]


def test_ingest_rejects_corrupt_batch_before_sending(tmp_path, capsys):
    bundle = tmp_path / "bundle"
    assert main([*GEN_ARGS, "--output-dir", str(bundle)]) == 0
    capsys.readouterr()
    victim = next(iter(sorted(bundle.glob("traces-*.jsonl"))))
    payload = victim.read_bytes()
    flipped = b"0" if payload[20:21] == b"1" else b"1"
    victim.write_bytes(payload[:20] + flipped + payload[21:])
    with Receiver() as receiver:
        assert run_ingest(bundle, receiver.endpoint) == 1
        assert "checksum" in capsys.readouterr().err
        assert receiver.requests == []


def test_ingest_rejects_missing_batch_before_sending(tmp_path, capsys):
    bundle = tmp_path / "bundle"
    assert main([*GEN_ARGS, "--output-dir", str(bundle)]) == 0
    capsys.readouterr()
    next(iter(sorted(bundle.glob("traces-*.jsonl")))).unlink()
    with Receiver() as receiver:
        assert run_ingest(bundle, receiver.endpoint) == 1
        assert "missing" in capsys.readouterr().err
        assert receiver.requests == []


def test_ingest_rejects_undeclared_batch_files(tmp_path, capsys):
    bundle = tmp_path / "bundle"
    assert main([*GEN_ARGS, "--output-dir", str(bundle)]) == 0
    capsys.readouterr()
    (bundle / "traces-000099.jsonl").write_bytes(b"{}\n")
    with Receiver() as receiver:
        assert run_ingest(bundle, receiver.endpoint) == 1
        assert "undeclared" in capsys.readouterr().err
        assert receiver.requests == []


def test_ingest_rejects_wrong_manifest_version(tmp_path, capsys):
    bundle = tmp_path / "bundle"
    assert main([*GEN_ARGS, "--output-dir", str(bundle)]) == 0
    capsys.readouterr()
    manifest = json.loads((bundle / "manifest.json").read_text())
    manifest["generator_version"] = 99
    (bundle / "manifest.json").write_text(json.dumps(manifest))
    with Receiver() as receiver:
        assert run_ingest(bundle, receiver.endpoint) == 1
        assert "generator_version" in capsys.readouterr().err
        assert receiver.requests == []


def test_rejected_spans_stop_ingestion_and_presume_checkpoint(tmp_path, capsys):
    bundle = tmp_path / "bundle"
    assert main([*GEN_ARGS, "--output-dir", str(bundle)]) == 0
    capsys.readouterr()

    def respond(index):
        if index == 2:
            return 200, {"partialSuccess": {"rejectedSpans": 3}}
        return 200, {"partialSuccess": {}}

    with Receiver(respond) as receiver:
        assert run_ingest(bundle, receiver.endpoint) == 1
    assert "rejected" in capsys.readouterr().err
    assert len(receiver.requests) == 3  # stopped immediately, no retry of the batch
    checkpoint = json.loads((bundle / ".ax-spans-ingest.json").read_text())
    assert checkpoint["next_batch"] == 2
    assert checkpoint["confirmed_spans"] == 4
    assert checkpoint["confirmed_bytes"] > 0


def test_resume_sends_only_unconfirmed_batches(tmp_path, capsys):
    bundle = tmp_path / "bundle"
    assert main([*GEN_ARGS, "--output-dir", str(bundle)]) == 0
    capsys.readouterr()
    lines = batch_lines(bundle)

    def die_from_third(index):
        if index >= 2:
            raise RuntimeError("port-forward died")
        return 200, {"partialSuccess": {}}

    with Receiver(die_from_third) as receiver:
        assert run_ingest(bundle, receiver.endpoint) == 1
        assert "ambiguously" in capsys.readouterr().err
        receiver.respond = lambda _index: (200, {"partialSuccess": {}})
        assert run_ingest(bundle, receiver.endpoint) == 0
    assert [body.rstrip(b"\n") for body in receiver.bodies[3:]] == [
        line.rstrip(b"\n") for line in lines[2:]
    ]
    summary = json.loads(capsys.readouterr().out)
    assert summary["spans"] == 12
    assert summary["batches"] == 6
    checkpoint = json.loads((bundle / ".ax-spans-ingest.json").read_text())
    assert checkpoint["next_batch"] == 6
    assert checkpoint["confirmed_spans"] == 12


def test_refuses_resume_against_a_different_destination(tmp_path, capsys):
    bundle = tmp_path / "bundle"
    assert main([*GEN_ARGS, "--output-dir", str(bundle)]) == 0
    capsys.readouterr()

    def die(index):
        if index >= 2:
            raise RuntimeError("port-forward died")
        return 200, {"partialSuccess": {}}

    with Receiver(die) as receiver:
        assert run_ingest(bundle, receiver.endpoint) == 1
    capsys.readouterr()
    with Receiver() as other:
        assert run_ingest(bundle, other.endpoint) == 1
        assert "different endpoint" in capsys.readouterr().err
        assert len(receiver.requests) == 3
        assert other.requests == []


def test_transport_retries_safe_connection_failures(monkeypatch):
    sleeps = []
    monkeypatch.setattr("spans.ingest.time.sleep", sleeps.append)
    refusals = [ConnectionRefusedError(), ConnectionRefusedError()]
    attempts = []

    def factory():
        attempts.append(1)
        return FakeConnection(refusals, [])

    transport = Transport(
        "http://localhost:4318/v1/traces",
        "space",
        "key",
        timeout=5,
        max_retries=3,
        connection_factory=factory,
    )
    assert transport.send(b"{}") == 2
    assert len(attempts) == 3
    assert len(sleeps) == 2
    assert all(0 < delay <= 1.0 for delay in sleeps)


def test_transport_exhausts_retries_without_progress(monkeypatch):
    monkeypatch.setattr("spans.ingest.time.sleep", lambda _delay: None)
    attempts = []

    def factory():
        attempts.append(1)
        return FakeConnection([ConnectionRefusedError()], [])

    transport = Transport(
        "http://localhost:4318/v1/traces",
        "space",
        "key",
        timeout=5,
        max_retries=2,
        connection_factory=factory,
    )
    with pytest.raises(SendError):
        transport.send(b"{}")
    assert len(attempts) == 3


def test_transport_honors_retry_after(monkeypatch):
    sleeps = []
    monkeypatch.setattr("spans.ingest.time.sleep", sleeps.append)
    pending = [FakeResponse(503, b"{}", {"Retry-After": "7"})]
    transport = Transport(
        "http://localhost:4318/v1/traces",
        "space",
        "key",
        timeout=5,
        max_retries=2,
        connection_factory=lambda: FakeConnection([], pending),
    )
    assert transport.send(b"{}") == 1
    assert sleeps == [7.0]


def test_transport_does_not_retry_ambiguous_failures(monkeypatch):
    monkeypatch.setattr("spans.ingest.time.sleep", lambda _delay: None)
    attempts = []

    def factory():
        attempts.append(1)
        return FakeConnection([], [TimeoutError("response lost")])

    transport = Transport(
        "http://localhost:4318/v1/traces",
        "space",
        "key",
        timeout=5,
        max_retries=5,
        connection_factory=factory,
    )
    with pytest.raises(SendError):
        transport.send(b"{}")
    assert len(attempts) == 1


class FakeResponse(http.client.HTTPResponse):
    def __init__(self, status, body, headers=None):
        self.fp = io.BufferedReader(io.BytesIO(b""))  # base __del__ closes self.fp
        self.status = status
        self.body = body
        self.response_headers = headers or {}

    def read(self, amt=None):  # noqa: ARG002 - stdlib signature
        return self.body

    def getheader(self, name, default=None):
        return self.response_headers.get(name, default)


class FakeConnection(http.client.HTTPConnection):
    """Scripted http.client stand-in for connect-phase and response failures."""

    def __init__(self, connect_failures, responses):
        self.connect_failures = connect_failures
        self.responses = responses

    def connect(self):
        if self.connect_failures:
            raise self.connect_failures.pop(0)

    def request(self, method, url, body=None, headers=None, *, encode_chunked=False):  # noqa: ARG002
        self.method, self.url, self.body = method, url, body

    def getresponse(self):
        outcome = self.responses.pop(0) if self.responses else None
        if isinstance(outcome, BaseException):
            raise outcome
        if outcome is not None:
            return outcome
        return FakeResponse(200, b'{"partialSuccess": {}}')

    def close(self):
        pass


def test_ingest_reports_missing_credentials(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("AX_SPANS_SPACE_KEY", raising=False)
    monkeypatch.delenv("AX_SPANS_API_KEY", raising=False)
    assert (
        main(["ingest", str(tmp_path), "--otlp-http-endpoint", "http://x/v1/traces"])
        == 1
    )
    assert "--space-key" in capsys.readouterr().err


def test_ingest_reads_credentials_from_environment(tmp_path, capsys, monkeypatch):
    bundle = tmp_path / "bundle"
    assert main([*GEN_ARGS, "--output-dir", str(bundle)]) == 0
    capsys.readouterr()
    monkeypatch.setenv("AX_SPANS_SPACE_KEY", "env-space")
    monkeypatch.setenv("AX_SPANS_API_KEY", "env-key")
    with Receiver() as receiver:
        code = main(["ingest", str(bundle), "--otlp-http-endpoint", receiver.endpoint])
        assert code == 0
        assert receiver.requests[0]["headers"]["space_key"] == "env-space"
        assert receiver.requests[0]["headers"]["api_key"] == "env-key"


def test_ingest_rejects_incomplete_endpoint(tmp_path, capsys):
    bundle = tmp_path / "bundle"
    assert main([*GEN_ARGS, "--output-dir", str(bundle)]) == 0
    capsys.readouterr()
    assert run_ingest(bundle, "localhost:4318") == 1
    assert "complete" in capsys.readouterr().err


def test_ingest_module_function_matches_cli_summary(tmp_path, capsys):
    bundle = tmp_path / "bundle"
    assert main([*GEN_ARGS, "--output-dir", str(bundle)]) == 0
    capsys.readouterr()
    with Receiver() as receiver:
        summary = ingest(
            bundle,
            receiver.endpoint,
            "space",
            "key",
            max_retries=1,
            timeout=5,
            stream=None,
        )
    assert summary["retries"] == 0
    capsys.readouterr()
