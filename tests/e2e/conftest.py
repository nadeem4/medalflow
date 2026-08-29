"""The projects the end-to-end suite compiles and runs, entirely offline (D6).

The healthy one is `examples/` -- the project a user is invited to copy, not a
private copy of it. That is deliberate: a second copy would drift, and the
copy that drifted would be the one people read. The example rots only by
turning this suite red.

The unhealthy ones stay under tests/fixtures. They are deliberately broken --
a model that raises, two models sharing a name, a dependency cycle -- and are
examples of nothing.
"""

import sys
from pathlib import Path

import pytest

from tests.conftest import OFFLINE_ENV

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = REPO_ROOT / "tests" / "fixtures"
EXAMPLE = REPO_ROOT / "examples"

PROJECT_PACKAGES = (
    "models",
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
    monkeypatch.syspath_prepend(str(EXAMPLE))

    def _configure(**overrides):
        for name in [m for m in sys.modules if m.split(".")[0] in PROJECT_PACKAGES]:
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
def example_project(project):
    """`examples/` itself: two bronze models, two silver, one gold."""
    return project(MEDALFLOW_MODELS_PACKAGE="models")


@pytest.fixture
def broken_project(project):
    """Three silver models broken three different ways, and one that works."""
    return project(MEDALFLOW_MODELS_PACKAGE="broken_project")
