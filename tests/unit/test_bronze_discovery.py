"""Bronze layer discovery (ADR 002, Decision 6).

Bronze is the last layer to be walked rather than derived. It reuses the same
package walk silver and gold do (`medallion/base/discovery.py`), differing only
in the attribute it looks for and the record it builds.

Bronze keys on the declared `name` rather than the class name: `bronze_metadata`
carries one, and it is what the plan reports and what the bronze table is called.
"""

import sys
import textwrap
from pathlib import Path

import pytest
from medalflow.medallion.bronze.metadata_discovery import BronzeMetadataDiscovery

MODEL = """
    from medalflow.medallion.bronze import BronzeSequencer, bronze_metadata


    @bronze_metadata(
        name="{name}", schema="bronze", source_system="d365", {extra}
    )
    class {cls}(BronzeSequencer):
        pass
"""


class _StubSettings:
    """What bronze discovery reads: nothing but its own existence.

    In particular bronze does *not* read `is_model_configured` -- see the test
    below.
    """

    def is_model_configured(self, model_name: str) -> bool:
        return False


def _write_bronze_package(root: Path, name: str, modules: dict[str, str]) -> None:
    package = root / name
    package.mkdir(parents=True, exist_ok=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    for module_name, source in modules.items():
        (package / f"{module_name}.py").write_text(textwrap.dedent(source), encoding="utf-8")


@pytest.fixture
def bronze_package(tmp_path, monkeypatch):
    """A throwaway bronze package on sys.path."""
    monkeypatch.syspath_prepend(str(tmp_path))

    def build(name: str, modules: dict[str, str]):
        _write_bronze_package(tmp_path, name, modules)
        for loaded in [m for m in sys.modules if m.split(".")[0] == name]:
            del sys.modules[loaded]
        discovery = BronzeMetadataDiscovery(name, settings=_StubSettings())
        # The global cache is keyed by layer, and these packages are throwaway.
        discovery._cache_manager = None
        return discovery

    yield build

    for loaded in [m for m in sys.modules if m.split(".")[0] in {"acme_bronze"}]:
        del sys.modules[loaded]


def test_discovery_walks_the_bronze_package(bronze_package):
    discovery = bronze_package(
        "acme_bronze", {"customers": MODEL.format(cls="Customers", name="Customers", extra="")}
    )

    discovered = discovery.discover_all()

    assert [model.name for model in discovered] == ["Customers"]
    assert discovered[0].sequencer_class.__name__ == "Customers"
    assert discovered[0].bronze_metadata.schema == "bronze"


def test_discovery_keys_on_the_declared_name_not_the_class_name(bronze_package):
    discovery = bronze_package(
        "acme_bronze",
        {"customers": MODEL.format(cls="AnyClassName", name="Customers", extra="")},
    )

    discovered = discovery.discover_all()

    assert [model.name for model in discovered] == ["Customers"]
    assert discovered[0].sequencer_class.__name__ == "AnyClassName"


def test_a_disabled_bronze_model_is_left_out(bronze_package):
    discovery = bronze_package(
        "acme_bronze",
        {
            "customers": MODEL.format(cls="Customers", name="Customers", extra=""),
            "retired": MODEL.format(cls="Retired", name="Retired", extra="disabled=True"),
        },
    )

    assert [model.name for model in discovery.discover_all()] == ["Customers"]


def test_bronze_discovery_does_not_filter_on_configured_models(bronze_package):
    """`configured_models` is silver's grouping concept, deliberately not bronze's.

    `_StubSettings.is_model_configured` answers False for everything. Applying
    it to bronze would drop every bronze model unless the deployment happened
    to list it under a setting documented as silver's -- the same reasoning
    that keeps the gate off gold.
    """
    discovery = bronze_package(
        "acme_bronze", {"customers": MODEL.format(cls="Customers", name="Customers", extra="")}
    )

    assert [model.name for model in discovery.discover_all()] == ["Customers"]


def test_the_imported_base_class_is_not_itself_a_model(bronze_package):
    """`BronzeSequencer` is imported into every model module and carries no
    declaration of its own, so it must never be discovered as a model."""
    discovery = bronze_package(
        "acme_bronze", {"customers": MODEL.format(cls="Customers", name="Customers", extra="")}
    )

    assert len(discovery.discover_all()) == 1


def test_an_empty_bronze_package_is_not_an_error(bronze_package):
    discovery = bronze_package("acme_bronze", {})

    assert discovery.discover_all() == []


def test_discovery_declares_the_layer_and_attribute_it_walks():
    assert BronzeMetadataDiscovery.layer == "bronze"
    assert BronzeMetadataDiscovery.metadata_attribute == "_bronze_metadata"
