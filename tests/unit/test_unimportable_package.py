"""A configured package that does not import is an error, not an empty layer.

`_walk_package` logged the ImportError and returned, so a mistyped
`MEDALFLOW_MODELS_PACKAGE` produced an empty plan and no complaint. A log line
is not a result: the caller sees zero models and has nothing to tell them why.

The distinction that has to survive: **"the package does not exist" and "the
package exists and declares nothing" are different answers, and only the first
is an error.** A project that uses two layers and leaves the third's package
importable-but-empty is a legitimate shape, and the fixtures rely on it.
"""

import sys
from pathlib import Path

import pytest
from medalflow.medallion.base.discovery import PackageNotImportable
from medalflow.medallion.bronze.metadata_discovery import BronzeMetadataDiscovery
from medalflow.medallion.gold.metadata_discovery import GoldMetadataDiscovery

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


class _StubSettings:
    """Only what discovery reads."""

    def is_model_configured(self, model_name: str) -> bool:
        return True


@pytest.fixture(autouse=True)
def fixtures_on_path(monkeypatch):
    monkeypatch.syspath_prepend(str(FIXTURES))
    for name in [m for m in sys.modules if m.split(".")[0] == "broken_project"]:
        del sys.modules[name]


def _discovery(cls, package):
    discovery = cls(package, settings=_StubSettings())
    discovery._cache_manager = None

    return discovery


def test_a_package_that_does_not_exist_raises():
    discovery = _discovery(BronzeMetadataDiscovery, "definitely_not_a_package")

    with pytest.raises(PackageNotImportable, match="definitely_not_a_package"):
        discovery.discover_all()


def test_the_error_names_the_variables_that_point_at_it():
    """The same shape of guidance as an unconfigured layer package."""
    discovery = _discovery(GoldMetadataDiscovery, "acme.typo")

    with pytest.raises(PackageNotImportable) as raised:
        discovery.discover_all()

    assert "MEDALFLOW_GOLD_PACKAGE" in str(raised.value)
    assert "MEDALFLOW_MODELS_PACKAGE" in str(raised.value)


def test_an_importable_package_declaring_nothing_is_not_an_error():
    """A layer a project does not use is a legitimate shape, not a mistake."""
    discovery = _discovery(GoldMetadataDiscovery, "broken_project.gold")

    assert discovery.discover_all() == []


def test_the_error_is_a_value_error():
    """Callers already guard discovery with ValueError."""
    assert issubclass(PackageNotImportable, ValueError)
