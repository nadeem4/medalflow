import logging
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FILTERS = PROJECT_ROOT / "src" / "core" / "logging" / "filters.py"

spec = spec_from_file_location("core.logging.filters_test", FILTERS)
filters = module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(filters)  # type: ignore[assignment]

ContextFilter = filters.ContextFilter
set_logging_context = filters.set_logging_context
set_request_context = filters.set_request_context
clear_request_context = filters.clear_request_context


def _record() -> logging.LogRecord:
    return logging.LogRecord(
        name="test.logger",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="sample",
        args=(),
        exc_info=None,
    )


def test_context_filter_respects_static_environment():
    set_logging_context(environment="qa", extra={"region": "us-east"})
    record = _record()
    assert ContextFilter().filter(record)
    assert getattr(record, "environment") == "qa"
    assert getattr(record, "region") == "us-east"


def test_context_filter_uses_request_context():
    set_logging_context(environment=None, extra=None)
    set_request_context(request_id="req-1", user_id="user-7")
    try:
        record = _record()
        assert ContextFilter().filter(record)
        assert record.request_id == "req-1"
        assert record.user_id == "user-7"
    finally:
        clear_request_context()


def test_context_filter_no_config_is_graceful():
    set_logging_context(environment=None, extra=None)
    record = _record()
    assert ContextFilter().filter(record)
    assert not hasattr(record, "environment")
