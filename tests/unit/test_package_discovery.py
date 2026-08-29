"""The package walk is layer-agnostic (ADR 002, Decision 6).

Silver was the only layer that could be discovered, and its walk was written
into `SilverMetadataDiscovery`. The walk is the same for every layer: import a
configured package, import each module under it, keep the classes carrying that
layer's metadata attribute. Only the package and the attribute differ.

These pin the behaviour the silver walk already had, now that gold shares it:
the `test` / `__pycache__` module skip, the `__module__` check that drops
imported classes, and a submodule that fails to import raising rather than
being skipped.

One of them has changed since. A missing root package used to be logged and
return nothing, so a mistyped package name produced an empty plan and no
complaint; it raises now. The pair below keeps the distinction that matters --
a package that does not exist is an error, a package that exists and declares
nothing is not.
"""

import sys
import textwrap
from pathlib import Path

import pytest
from medalflow.medallion.base.discovery import PackageNotImportable, _BaseDiscovery


class _StubSettings:
    def package_for_layer(self, layer):
        return f"configured.{layer}"


class _Probe(_BaseDiscovery):
    """A discovery that keeps every decorated class it is handed.

    `seen` records what the walk actually offered it, qualified by defining
    module -- the deduplicating `name` key alone would hide a class being
    visited twice.
    """

    layer = "silver"
    metadata_attribute = "_probe_metadata"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen: list[str] = []

    def _extract_metadata_from_class(self, cls):
        self.seen.append(f"{cls.__module__}.{cls.__name__}")
        return cls._probe_metadata


def _write_package(root: Path, name: str, modules: dict[str, str]) -> None:
    package = root / name
    package.mkdir(parents=True, exist_ok=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    for module_name, source in modules.items():
        (package / f"{module_name}.py").write_text(textwrap.dedent(source), encoding="utf-8")


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A throwaway package on sys.path, torn down after each test."""
    monkeypatch.syspath_prepend(str(tmp_path))

    def build(name: str, modules: dict[str, str]):
        _write_package(tmp_path, name, modules)
        for loaded in [m for m in sys.modules if m.split(".")[0] == name]:
            del sys.modules[loaded]
        discovery = _Probe(name, settings=_StubSettings())
        # The global cache is keyed by layer, so a cached result would outlive
        # the throwaway package it was discovered from. These are walk tests.
        discovery._cache_manager = None
        return discovery

    yield build

    built = {"walkable", "broken", "shadowed", "empty"}
    for loaded in [m for m in sys.modules if m.split(".")[0] in built]:
        del sys.modules[loaded]


DECORATED = """
    class Meta:
        def __init__(self, name):
            self.name = name

    class {cls}:
        _probe_metadata = Meta("{name}")
"""


def test_the_walk_finds_decorated_classes_across_submodules(project):
    discovery = project(
        "walkable",
        {
            "one": DECORATED.format(cls="First", name="first"),
            "two": DECORATED.format(cls="Second", name="second"),
        },
    )

    assert sorted(m.name for m in discovery.discover_all()) == ["first", "second"]


def test_a_class_imported_into_a_module_is_not_discovered_twice(project):
    """The `__module__` check: only the module that defines a class owns it."""
    discovery = project(
        "shadowed",
        {
            "definitions": DECORATED.format(cls="Only", name="only"),
            "reexport": "from shadowed.definitions import Only  # noqa: F401",
        },
    )

    discovered = discovery.discover_all()

    assert [m.name for m in discovered] == ["only"]
    assert discovery.seen == ["shadowed.definitions.Only"]


def test_modules_that_look_like_tests_are_skipped(project):
    discovery = project(
        "walkable",
        {
            "model": DECORATED.format(cls="Real", name="real"),
            "test_model": DECORATED.format(cls="Fake", name="fake"),
        },
    )

    assert [m.name for m in discovery.discover_all()] == ["real"]
    assert discovery.seen == ["walkable.model.Real"]


def test_a_missing_root_package_raises_rather_than_discovering_nothing():
    """It used to log and return `[]`, which the caller could not tell apart
    from a project that genuinely declares no models."""
    discovery = _Probe("no_such_package_anywhere", settings=_StubSettings())
    discovery._cache_manager = None

    with pytest.raises(PackageNotImportable, match="no_such_package_anywhere"):
        discovery.discover_all()


def test_a_root_package_that_imports_and_declares_nothing_is_not_an_error(project):
    """The other half of the distinction: an empty layer is a real shape."""
    discovery = project("empty", {})

    assert discovery.discover_all() == []


def test_a_submodule_that_fails_to_import_raises_and_names_the_module(project):
    discovery = project(
        "broken",
        {
            "good": DECORATED.format(cls="Good", name="good"),
            "bad": "raise RuntimeError('boom')",
        },
    )

    with pytest.raises(ValueError) as error:
        discovery.discover_all()

    assert "broken.bad" in str(error.value)


# --- the package comes from settings when not passed -----------------------


def test_the_package_defaults_to_the_configured_one_for_the_layer():
    discovery = _Probe(settings=_StubSettings())

    assert discovery.package == "configured.silver"
