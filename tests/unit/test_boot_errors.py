"""What MedalFlow says when there is not enough configuration to boot.

Settings are pydantic, and an unconfigured process used to get pydantic's own
report::

    source_system / ds_env / name / compute -- Field required

Those are field names. A user has no way to get from `compute` to
`MEDALFLOW_COMPUTE__LAKE_DATABASE_NAME`, which is the one they actually have
to set. The settings object knows its own `env_prefix` and
`env_nested_delimiter`, so the translation is derivable -- and derived, rather
than a hand-maintained table that rots the first time a field moves.

`ValueError` is what these assert on rather than a MedalFlow-specific class:
pydantic's own `ValidationError` is one, so nothing here is asserting that the
message got wrapped, only that it got readable.

The boot contract itself is unchanged: four variables, exactly as before.
"""

import os
import re

import pytest
from medalflow.settings import main as settings_main
from medalflow.settings.main import MedalflowSettings


@pytest.fixture
def unconfigured(monkeypatch, tmp_path):
    """A process with no MedalFlow configuration at all.

    An empty working directory too: settings read `.env` from the directory
    they are booted in.
    """
    for key in [k for k in os.environ if k.startswith(("MEDALFLOW_", "CTE_"))]:
        monkeypatch.delenv(key, raising=False)

    monkeypatch.chdir(tmp_path)

    try:
        yield
    finally:
        settings_main._settings = None


def _boot_failure(_unconfigured) -> str:
    with pytest.raises(ValueError) as excinfo:
        settings_main.get_settings(force_reload=True)

    return str(excinfo.value)


def test_the_three_top_level_variables_are_named(unconfigured):
    message = _boot_failure(unconfigured)

    assert "MEDALFLOW_SOURCE_SYSTEM" in message
    assert "MEDALFLOW_DS_ENV" in message
    assert "MEDALFLOW_NAME" in message


def test_a_missing_group_names_the_variable_inside_it_not_the_group(unconfigured):
    """`compute` alone is the actively unhelpful one: it is a pydantic model,
    and what the user must set is the required field inside it."""
    message = _boot_failure(unconfigured)

    assert "MEDALFLOW_COMPUTE__LAKE_DATABASE_NAME" in message


def test_the_message_names_environment_variables_and_nothing_else(unconfigured):
    """No bare pydantic field names survive into what the user reads."""
    message = _boot_failure(unconfigured)
    named = re.findall(r"^\s+(\S+?):", message, re.MULTILINE)

    assert named
    assert [name for name in named if not name.startswith("MEDALFLOW_")] == []


def test_the_names_are_derived_from_the_settings_own_config(unconfigured):
    """`env_prefix` and `env_nested_delimiter` live on the settings object.
    Reading them is what stops this being a lookup table that rots."""
    config = MedalflowSettings.model_config
    derived = (
        f"{config['env_prefix']}compute{config['env_nested_delimiter']}lake_database_name"
    ).upper()

    assert derived in _boot_failure(unconfigured)


def test_setting_exactly_what_the_message_names_boots(unconfigured, monkeypatch):
    """The list has to be sufficient as well as correct -- a user who does
    what it says ends up with settings. Still four variables: the boot
    contract did not change, only what is said about it."""
    message = _boot_failure(unconfigured)
    named = re.findall(r"^\s+(MEDALFLOW_\S+?):", message, re.MULTILINE)

    assert len(named) == 4
    for name in named:
        monkeypatch.setenv(name, "x")

    settings = settings_main.get_settings(force_reload=True)

    assert settings.name == "x"
    assert settings.compute.lake_database_name == "x"
