"""Regression tests for the observability consolidation (Phase 2, batch B5).

`operation_instrumentation` opened a span with the same name as the one
`execution_request_scope` had just opened, nesting a duplicate child under
every instrumented operation: double the span volume, attributes split across
the pair, and each exception recorded twice (once by each span).
"""

import pytest
from opentelemetry import trace

from core.observability.context import ExecutionRequestContext, execution_request_scope


class _RecordingSpan:
    def __init__(self, name):
        self.name = name
        self.attributes = {}

    def set_attribute(self, key, value):
        self.attributes[key] = value

    def record_exception(self, exc):
        pass

    def set_status(self, status):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _RecordingTracer:
    def __init__(self, opened):
        self._opened = opened

    def start_as_current_span(self, name):
        span = _RecordingSpan(name)
        self._opened.append(span)
        return span


@pytest.fixture
def opened_spans(monkeypatch):
    spans = []
    monkeypatch.setattr(trace, "get_tracer", lambda *a, **k: _RecordingTracer(spans))
    return spans


def test_request_scope_opens_exactly_one_span(opened_spans):
    ctx = ExecutionRequestContext(request_id="req-1")

    with execution_request_scope(ctx, operation="medalflow.operation.load"):
        pass

    assert [s.name for s in opened_spans] == ["medalflow.operation.load"]


def test_request_scope_yields_its_span_for_reuse(opened_spans):
    """Instrumentation reuses this span rather than opening a duplicate."""
    ctx = ExecutionRequestContext(request_id="req-1")

    with execution_request_scope(ctx, operation="medalflow.operation.load") as span:
        span.set_attribute("medalflow.custom", "value")

    assert len(opened_spans) == 1
    assert opened_spans[0].attributes["medalflow.custom"] == "value"
    assert opened_spans[0].attributes["medalflow.request_id"] == "req-1"


def test_request_scope_records_an_exception_once(opened_spans):
    ctx = ExecutionRequestContext(request_id="req-1")

    with pytest.raises(RuntimeError):
        with execution_request_scope(ctx, operation="medalflow.operation.load"):
            raise RuntimeError("boom")

    assert len(opened_spans) == 1


class _FakeInstrument:
    def __init__(self):
        self.calls = []

    def add(self, value, attrs):
        self.calls.append((value, attrs))

    def record(self, value, attrs):
        self.calls.append((value, attrs))


class _FakeMetrics:
    def __init__(self):
        self.operation_counter = _FakeInstrument()
        self.duration_histogram = _FakeInstrument()


def test_instrumented_operation_opens_one_span_not_two(opened_spans):
    """The duplicate was a same-named child span under every operation."""
    from core.observability.instrumentation import operation_instrumentation

    ctx = ExecutionRequestContext(request_id="req-1")
    metrics = _FakeMetrics()

    with operation_instrumentation(
        ctx=ctx, metrics=metrics, stage_name="1", operation_name="load"
    ):
        pass

    assert [s.name for s in opened_spans] == ["medalflow.operation.load"]
    assert len(metrics.operation_counter.calls) == 1
