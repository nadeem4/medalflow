"""What the shared package walk must never do quietly (ADR 002, Decision 6).

`_BaseDiscovery` is one implementation under all three layers, and `compile()`
and `run()` are built on top of it. Two of its behaviours dropped models from
the plan without saying anything:

1. `_is_model_class` asked `hasattr`, which is true for an *inherited*
   attribute. A class subclassing a decorated model in another module was
   therefore discovered as if it carried its own declaration. In bronze and
   silver, where `name` comes from the metadata, the subclass inherits the same
   name and *displaces its parent* in the results -- so the plan silently loses
   the real model and gains a class nobody declared. The `__module__` guard does
   not help: the subclass genuinely lives in its own module.

2. `discovered` is keyed by name, so two models sharing one name collapsed to
   whichever the walk reached last. No error, no warning, one model simply
   missing.

Both are the defect class this phase exists to remove, so both are pinned here
for every layer rather than for whichever one happened to surface them.
"""

import sys
import textwrap

import pytest
from medalflow.medallion.bronze.metadata_discovery import BronzeMetadataDiscovery
from medalflow.medallion.gold.metadata_discovery import GoldMetadataDiscovery
from medalflow.medallion.silver.metadata_discovery import SilverMetadataDiscovery

PACKAGE = "acme_models"

BRONZE_MODEL = """
    from medalflow.medallion.bronze import BronzeSequencer, bronze_metadata


    @bronze_metadata(name="{name}", schema="bronze", source_system="d365")
    class {cls}(BronzeSequencer):
        pass
"""

SILVER_MODEL = """
    from medalflow.medallion.silver import SilverTransformationSequencer, silver_metadata


    @silver_metadata(name="{name}", model="sales")
    class {cls}(SilverTransformationSequencer):
        pass
"""

# Gold takes its identity from the class name, so `name` is unused here and a
# duplicate is two classes sharing a class name in different modules.
GOLD_MODEL = """
    from medalflow.medallion.gold import GoldSequencer, gold_metadata


    @gold_metadata(schema="gold")
    class {cls}(GoldSequencer):
        pass
"""

SUBCLASS = """
    from acme_models.a_base import {parent}


    class {cls}({parent}):
        pass
"""

DECORATED_SUBCLASS = {
    "bronze": """
    from acme_models.a_base import Customers
    from medalflow.medallion.bronze import bronze_metadata


    @bronze_metadata(name="Archived", schema="bronze", source_system="d365")
    class Archived(Customers):
        pass
""",
    "silver": """
    from acme_models.a_base import Customers
    from medalflow.medallion.silver import silver_metadata


    @silver_metadata(name="Archived", model="sales")
    class Archived(Customers):
        pass
""",
    "gold": """
    from acme_models.a_base import Customers
    from medalflow.medallion.gold import gold_metadata


    @gold_metadata(schema="gold")
    class Archived(Customers):
        pass
""",
}

LAYERS = {
    "bronze": (BronzeMetadataDiscovery, BRONZE_MODEL, "Duplicate"),
    "silver": (SilverMetadataDiscovery, SILVER_MODEL, "Duplicate"),
    # Gold keys on the class name, so its duplicate has to reuse it.
    "gold": (GoldMetadataDiscovery, GOLD_MODEL, "Customers"),
}


class _StubSettings:
    """Silver filters on this; bronze and gold deliberately do not."""

    def is_model_configured(self, model_name: str) -> bool:
        return model_name == "sales"


@pytest.fixture(params=sorted(LAYERS), ids=sorted(LAYERS))
def layer(request, tmp_path, monkeypatch):
    """A throwaway model package for one layer, plus its discovery."""
    discovery_class, model_template, second_class = LAYERS[request.param]
    monkeypatch.syspath_prepend(str(tmp_path))

    def build(modules: dict[str, str]):
        package = tmp_path / PACKAGE
        package.mkdir(parents=True, exist_ok=True)
        (package / "__init__.py").write_text("", encoding="utf-8")
        for module_name, source in modules.items():
            (package / f"{module_name}.py").write_text(textwrap.dedent(source), encoding="utf-8")

        for loaded in [m for m in sys.modules if m.split(".")[0] == PACKAGE]:
            del sys.modules[loaded]

        discovery = discovery_class(PACKAGE, settings=_StubSettings())
        # The global cache is keyed by layer, and these packages are throwaway.
        discovery._cache_manager = None
        return discovery

    build.model = model_template
    build.second_class = second_class
    build.layer = request.param

    yield build

    for loaded in [m for m in sys.modules if m.split(".")[0] == PACKAGE]:
        del sys.modules[loaded]


# --- 1. an inherited decorator is not a declaration ------------------------


def test_a_subclass_of_a_model_is_not_itself_a_model(layer):
    """The outcome, not the mechanism: the declared model must survive.

    `a_base` is walked before `b_derived`, so the subclass -- carrying its
    parent's inherited name -- overwrote the real model in the results. The
    plan lost `Customers` and gained `Derived` in its place.
    """
    discovery = layer(
        {
            "a_base": layer.model.format(cls="Customers", name="Customers"),
            "b_derived": SUBCLASS.format(parent="Customers", cls="Derived"),
        }
    )

    discovered = discovery.discover_all()

    assert [model.sequencer_class.__name__ for model in discovered] == ["Customers"]


def test_the_declared_model_keeps_its_name_when_subclassed(layer):
    discovery = layer(
        {
            "a_base": layer.model.format(cls="Customers", name="Customers"),
            "b_derived": SUBCLASS.format(parent="Customers", cls="Derived"),
        }
    )

    assert [model.name for model in discovery.discover_all()] == ["Customers"]


def test_a_subclass_that_declares_its_own_metadata_is_discovered(layer):
    """The fix must not go too far: re-decorating a subclass is legitimate."""
    discovery = layer(
        {
            "a_base": layer.model.format(cls="Customers", name="Customers"),
            "b_derived": DECORATED_SUBCLASS[layer.layer],
        }
    )

    discovered = sorted(model.sequencer_class.__name__ for model in discovery.discover_all())

    assert discovered == ["Archived", "Customers"]


# --- 2. a duplicated name is an authoring error ----------------------------


def test_two_models_sharing_a_name_raise(layer):
    discovery = layer(
        {
            "a_first": layer.model.format(cls="Customers", name="Customers"),
            "b_second": layer.model.format(cls=layer.second_class, name="Customers"),
        }
    )

    with pytest.raises(ValueError, match="Customers"):
        discovery.discover_all()


def test_the_duplicate_error_names_both_classes(layer):
    """An author has to be able to find both ends of the collision."""
    discovery = layer(
        {
            "a_first": layer.model.format(cls="Customers", name="Customers"),
            "b_second": layer.model.format(cls=layer.second_class, name="Customers"),
        }
    )

    with pytest.raises(ValueError) as error:
        discovery.discover_all()

    message = str(error.value)
    assert f"{PACKAGE}.a_first.Customers" in message
    assert f"{PACKAGE}.b_second.{layer.second_class}" in message


def test_one_model_per_name_is_still_fine(layer):
    discovery = layer(
        {
            "a_first": layer.model.format(cls="Customers", name="Customers"),
            "b_second": layer.model.format(cls="Orders", name="Orders"),
        }
    )

    assert sorted(model.name for model in discovery.discover_all()) == ["Customers", "Orders"]
