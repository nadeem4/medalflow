"""Configuration for MedalFlow.

Everything MedalFlow reads lives on one object, :class:`MedalflowSettings`.
Identity is declared once at the top level; the rest is grouped by domain into
plain pydantic models.

Environment variables
---------------------
Every variable is prefixed ``MEDALFLOW_``. Use the ``__`` delimiter to descend
into a group::

    MEDALFLOW_NAME=fin                              # top level
    MEDALFLOW_COMPUTE__LAKE_DATABASE_NAME=lakedb    # compute group
    MEDALFLOW_DATALAKE__PROCESSED__ACCOUNT_NAME=mylake

Values are read from the process environment and from a ``.env`` file, in that
precedence order, ahead of the defaults declared in code. Secrets (ODBC strings,
lake access keys) are never environment variables: they are pulled from Azure
Key Vault by name.

Minimal configuration
---------------------
Four variables are enough to construct settings::

    MEDALFLOW_SOURCE_SYSTEM      # source ERP system, e.g. sap
    MEDALFLOW_DS_ENV             # data source environment, e.g. dev
    MEDALFLOW_NAME               # short data source name, e.g. fin
    MEDALFLOW_COMPUTE__LAKE_DATABASE_NAME

A real deployment adds two more -- the processed lake account and the Key Vault
that holds its credentials::

    MEDALFLOW_DATALAKE__PROCESSED__ACCOUNT_NAME
    MEDALFLOW_KEYVAULT__URL

See ``.env.example`` at the repository root. For the authoritative list of every
option, read the settings classes themselves; each field carries its own
description.

Groups
------
- ``compute``  -- SQL platform, pools, external data sources (``compute.py``)
- ``datalake`` -- processed and internal ADLS accounts (``datalake.py``)
- ``keyvault`` -- Azure Key Vault connection and credentials (``keyvault.py``)
- ``features`` -- feature flags (``features.py``)
- ``stats``    -- statistics management (``stats.py``)

Quick start
-----------
    >>> from medalflow.settings import get_settings
    >>>
    >>> settings = get_settings()
    >>> settings.table_prefix
    'fin_'
    >>> settings.compute.active_config.dialect
    'tsql'
"""

# Re-exported for use inside the package only; deliberately kept out of __all__
# so the documented public surface stays limited to get_settings. noqa: F401 is
# required because ruff cannot see the cross-module consumers of these names.
from medalflow.constants.compute import ComputeEnvironment  # noqa: F401

from .compute import ComputeSettings  # noqa: F401
from .main import (
    MedalflowSettings,  # noqa: F401
    get_settings,
)

__all__ = [
    # Public API - only expose the settings accessor
    "get_settings",
]
