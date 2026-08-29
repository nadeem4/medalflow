"""Docstrings that describe things which do not exist, pinned.

A docstring is the one part of the source nothing verifies, so it is the part
that survives the deletion of what it describes. Phase 2 removed the Spark
engine and every non-Synapse platform; the prose advertising them outlived the
code by two phases. These tests fail the moment it comes back.

They are deliberately narrow. ``EngineType.SPARK`` is retained on purpose and
its docstring explains why, and ``spark`` is also a real sqlglot dialect name --
neither is a claim about a compute engine, so neither is matched here.
"""

import importlib
import pkgutil

import medalflow

# Names Phase 2 deleted. Each one was advertised in a docstring after the class
# it named was gone, which is the failure mode these tests exist to catch.
DELETED_ENGINES = ("SynapseSparkEngine", "SnowflakeSQLEngine", "DatabricksSQLEngine")


def _every_module():
    """Import every module in the package and yield ``(name, module)``."""
    yield medalflow.__name__, medalflow
    for info in pkgutil.walk_packages(medalflow.__path__, f"{medalflow.__name__}."):
        yield info.name, importlib.import_module(info.name)


def _every_docstring():
    """Yield ``(where, text)`` for every module docstring in the package."""
    for name, module in _every_module():
        if module.__doc__:
            yield name, module.__doc__


def test_no_docstring_advertises_an_engine_that_was_deleted():
    offenders = [
        f"{where}: {engine}"
        for where, text in _every_docstring()
        for engine in DELETED_ENGINES
        if engine in text
    ]

    assert offenders == []


def test_the_deleted_engines_really_are_gone():
    """The other test is only meaningful while these names import nowhere."""
    for _, module in _every_module():
        for engine in DELETED_ENGINES:
            assert not hasattr(module, engine)


def test_every_public_module_has_a_docstring():
    """mkdocstrings renders a module with no docstring as a bare list of
    symbols, which is how the fictional ones went unnoticed for two phases."""
    missing = [name for name, module in _every_module() if not module.__doc__]

    assert missing == []


def test_the_package_docstring_only_names_what_it_exports():
    """`help(medalflow)` is the first thing a reader meets. Every name it
    presents in a definition list is checked against ``__all__``, so trimming
    the public surface cannot leave the introduction describing the old one."""
    import re

    named = set(re.findall(r"``([A-Za-z_][A-Za-z_0-9]*)``", medalflow.__doc__))

    assert named
    assert named <= set(medalflow.__all__)
