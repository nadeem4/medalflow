"""Fixture projects for the end-to-end suite.

`compile()` and `run()` are both asked the same question -- what does this
project do -- so they point at the same fixture projects under
tests/fixtures, entirely offline (D6).
"""

import sys
from pathlib import Path

import pytest

from tests.conftest import OFFLINE_ENV

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"

FIXTURE_PACKAGES = (
    "sample_project",
    "broken_project",
    "duplicate_project",
    "cyclic_project",
)


@pytest.fixture
def project(monkeypatch):
    """Point real settings at a fixture project, offline.

    Yields a callable taking the settings overrides a project needs, so each
    test states its own configuration -- which is what makes 'this layer has
    no package configured' testable at all.
    """
    from medalflow.settings import main as settings_main

    monkeypatch.syspath_prepend(str(FIXTURES))

    def _configure(**overrides):
        for name in [m for m in sys.modules if m.split(".")[0] in FIXTURE_PACKAGES]:
            del sys.modules[name]

        # Nothing below sets it, and that is the point: an unset model list
        # filters nothing. A developer with it exported would otherwise be
        # testing a different contract from CI.
        monkeypatch.delenv("MEDALFLOW_CONFIGURED_MODELS", raising=False)

        for key, value in {**OFFLINE_ENV, **overrides}.items():
            monkeypatch.setenv(key, value)

        return settings_main.get_settings(force_reload=True)

    try:
        yield _configure
    finally:
        # The singleton outlives monkeypatch's env cleanup.
        settings_main._settings = None


@pytest.fixture
def sample_project(project):
    """The five-model project: two bronze, two silver, one gold."""
    return project(MEDALFLOW_MODELS_PACKAGE="sample_project")


@pytest.fixture
def broken_project(project):
    """Three silver models broken three different ways, and one that works."""
    return project(MEDALFLOW_MODELS_PACKAGE="broken_project")
