"""Gold layer discovery (ADR 002, Decision 6).

Gold's public entry point built a *bare* `GoldSequencer`. A bare instance
carries no `@query_metadata` methods of its own, so the operations list came
back empty and the orchestrator raised on it — a user's `@gold_metadata` class
was never found. Gold discovers its models by package walk now, the same way
silver does.
"""

import sys
import textwrap
from pathlib import Path

import pytest
from medalflow.settings import main as settings_main

from tests.conftest import OFFLINE_ENV

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"

# The offline four, plus the one variable that says where the models live.
GOLD_ENV = OFFLINE_ENV | {"MEDALFLOW_MODELS_PACKAGE": "sample_project"}


@pytest.fixture
def sample_project_settings(monkeypatch):
    """Point MedalFlow at the sample project the way a real deployment would."""
    monkeypatch.syspath_prepend(str(FIXTURES))
    for name in [m for m in sys.modules if m.split(".")[0] == "sample_project"]:
        del sys.modules[name]

    for key, value in GOLD_ENV.items():
        monkeypatch.setenv(key, value)

    settings = settings_main.get_settings(force_reload=True)
    try:
        yield settings
    finally:
        settings_main._settings = None


def test_gold_plan_finds_the_decorated_gold_model(sample_project_settings):
    """The bug: this raised `Cannot create execution plan from empty operations list`."""
    from medalflow.api.medallion import get_gold_execution_plan

    plan = get_gold_execution_plan(None)

    assert [operation.object_name for stage in plan.stages for operation in stage.operations] == [
        "vw_Revenue"
    ]


# --- discovery over a gold package -----------------------------------------


class _StubSettings:
    """What gold discovery reads: nothing but its own existence.

    In particular gold does *not* read `is_model_configured` -- see the test
    below.
    """

    def is_model_configured(self, model_name: str) -> bool:
        return False


def _write_gold_package(root: Path, name: str, modules: dict[str, str]) -> None:
    package = root / name
    package.mkdir(parents=True, exist_ok=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    for module_name, source in modules.items():
        (package / f"{module_name}.py").write_text(textwrap.dedent(source), encoding="utf-8")


MODEL = """
    from medalflow.constants.sql import QueryType
    from medalflow.medallion.base.decorators import query_metadata
    from medalflow.medallion.gold import GoldSequencer, gold_metadata


    @gold_metadata(schema="gold", {extra})
    class {cls}(GoldSequencer):
        @query_metadata(
            type=QueryType.CREATE_OR_ALTER_VIEW,
            table_name="vw_{cls}",
            schema_name="gold",
        )
        def build_view(self) -> str:
            return "SELECT 1 AS One FROM silver.FactOrders"
"""


@pytest.fixture
def gold_package(tmp_path, monkeypatch):
    """A throwaway gold package on sys.path."""
    from medalflow.medallion.gold.metadata_discovery import GoldMetadataDiscovery

    monkeypatch.syspath_prepend(str(tmp_path))

    def build(name: str, modules: dict[str, str]):
        _write_gold_package(tmp_path, name, modules)
        for loaded in [m for m in sys.modules if m.split(".")[0] == name]:
            del sys.modules[loaded]
        discovery = GoldMetadataDiscovery(name, settings=_StubSettings())
        # The global cache is keyed by layer, and these packages are throwaway.
        discovery._cache_manager = None
        return discovery

    yield build

    for loaded in [m for m in sys.modules if m.split(".")[0] in {"acme_gold"}]:
        del sys.modules[loaded]


def test_discovery_finds_a_decorated_gold_model(gold_package):
    discovery = gold_package("acme_gold", {"revenue": MODEL.format(cls="Revenue", extra="")})

    discovered = discovery.discover_all()

    assert [model.name for model in discovered] == ["Revenue"]
    assert discovered[0].sequencer_class.__name__ == "Revenue"
    assert discovered[0].gold_metadata.schema == "gold"


def test_a_disabled_gold_model_is_left_out(gold_package):
    discovery = gold_package(
        "acme_gold",
        {
            "revenue": MODEL.format(cls="Revenue", extra=""),
            "retired": MODEL.format(cls="Retired", extra="disabled=True"),
        },
    )

    assert [model.name for model in discovery.discover_all()] == ["Revenue"]


def test_gold_discovery_does_not_filter_on_configured_models(gold_package):
    """`configured_models` is silver's grouping concept, deliberately not gold's.

    `_StubSettings.is_model_configured` answers False for everything. Applying
    it to gold would drop every gold model unless the deployment happened to
    list it under a setting documented as silver's.
    """
    discovery = gold_package("acme_gold", {"revenue": MODEL.format(cls="Revenue", extra="")})

    assert [model.name for model in discovery.discover_all()] == ["Revenue"]


def test_gold_metadata_defaults_to_enabled():
    from medalflow.medallion.gold import gold_metadata

    @gold_metadata(schema="gold")
    class Anything:
        pass

    assert Anything._gold_metadata.disabled is False
