"""Regression tests for fail-fast plan generation (Phase 1, task 9).

Two blanket handlers turned authoring mistakes into empty results:

- `_BaseSequencer.get_queries` caught every exception, logged it, and returned
  `[]`. A model whose query method raised produced a plan with zero operations
  and no indication anything was wrong.
- `_walk_silver_package` caught every import error at *debug* level and
  continued, so a syntax error in a user model silently yielded zero
  discovered transformations.

dbt-style: authoring mistakes fail loudly at plan time.
"""

import logging
import sys
import textwrap

import pytest

from medalflow.medallion.base.sequencer import _BaseSequencer
from medalflow.medallion.silver.metadata_discovery import SilverMetadataDiscovery


class _ExplodingSequencer(_BaseSequencer):
    """Sequencer whose discovery blows up, as a broken user model would."""

    def __init__(self):
        self.logger = logging.getLogger("exploding-sequencer")

    def get_obj_name(self) -> str:
        return "CustomerModel"

    def get_layer_name(self) -> str:
        return "silver"

    def _discover_methods(self):
        raise RuntimeError("bad SQL in build_customer")


def test_get_queries_raises_instead_of_returning_empty():
    with pytest.raises(Exception) as excinfo:
        _ExplodingSequencer().get_queries()

    message = str(excinfo.value)
    assert "CustomerModel" in message or "_ExplodingSequencer" in message
    assert "bad SQL in build_customer" in message


def test_get_queries_preserves_the_original_error():
    with pytest.raises(Exception) as excinfo:
        _ExplodingSequencer().get_queries()

    assert isinstance(excinfo.value.__cause__, RuntimeError)


def _write_package(tmp_path, monkeypatch, name, modules):
    package = tmp_path / name
    package.mkdir()
    (package / "__init__.py").write_text("")
    for module_name, source in modules.items():
        (package / f"{module_name}.py").write_text(textwrap.dedent(source))

    monkeypatch.syspath_prepend(str(tmp_path))
    for loaded in [m for m in sys.modules if m == name or m.startswith(name + ".")]:
        del sys.modules[loaded]
    return name


@pytest.fixture
def discovery_factory(monkeypatch):
    def build(package_name):
        instance = SilverMetadataDiscovery.__new__(SilverMetadataDiscovery)
        instance.settings = None
        instance.silver_package = package_name
        instance.logger = logging.getLogger("test-discovery")
        instance._cache_manager = None
        return instance

    return build


def test_a_model_with_a_syntax_error_fails_discovery_naming_the_module(
    tmp_path, monkeypatch, discovery_factory
):
    package = _write_package(
        tmp_path,
        monkeypatch,
        "broken_silver",
        {"customers": "def build(:\n    pass\n"},
    )

    with pytest.raises(Exception) as excinfo:
        list(discovery_factory(package)._walk_silver_package())

    assert "broken_silver.customers" in str(excinfo.value)


def test_a_model_with_a_bad_import_fails_discovery_naming_the_module(
    tmp_path, monkeypatch, discovery_factory
):
    package = _write_package(
        tmp_path,
        monkeypatch,
        "badimport_silver",
        {"orders": "import a_module_that_does_not_exist\n"},
    )

    with pytest.raises(Exception) as excinfo:
        list(discovery_factory(package)._walk_silver_package())

    assert "badimport_silver.orders" in str(excinfo.value)


def test_a_healthy_package_still_walks(tmp_path, monkeypatch, discovery_factory):
    package = _write_package(
        tmp_path,
        monkeypatch,
        "healthy_silver",
        {"customers": "VALUE = 1\n"},
    )

    modules = list(discovery_factory(package)._walk_silver_package())

    assert [m.__name__ for m in modules] == ["healthy_silver.customers"]
