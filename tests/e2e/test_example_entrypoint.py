"""`examples/run.py` -- the quickstart's one command, actually run.

The README tells a reader to type `python run.py` in `examples/`, and nothing
else in the suite executes that file. A subprocess is the point: it proves the
script works from a shell in the example's own directory with only the
documented environment set, rather than proving its imports resolve under
pytest's carefully arranged `sys.path`.

`PYTHONPATH` is the one thing here a reader does not do: it stands in for the
`pip install medalflow` the README asks for, so the suite runs against the
source tree it is testing. Everything else -- the working directory and the
five variables -- is what the README says, verbatim.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = REPO_ROOT / "examples"

# The four variables that construct settings, plus the one discovery needs,
# and nothing else. There is no Key Vault here and no storage account: that is
# the assertion, not an omission. Compiling is offline (D6) because bronze
# models are declared, and if that ever stopped being the default this file is
# where it surfaces -- as a run reaching for a warehouse it was never told
# about.
EXAMPLE_ENV = {
    "MEDALFLOW_SOURCE_SYSTEM": "d365",
    "MEDALFLOW_DS_ENV": "dev",
    "MEDALFLOW_NAME": "sales",
    "MEDALFLOW_COMPUTE__LAKE_DATABASE_NAME": "lakedb",
    "MEDALFLOW_MODELS_PACKAGE": "models",
}

# Enough of the OS environment for the interpreter to start, and no MedalFlow
# variable a developer happens to have exported.
_OS_KEYS = ("PATH", "SYSTEMROOT", "TEMP", "TMP", "COMSPEC", "HOME")


@pytest.fixture
def entrypoint():
    """Run `python run.py` in `examples/`, offline, and hand back the result."""
    return subprocess.run(
        [sys.executable, "run.py"],
        cwd=EXAMPLE,
        env={
            **{key: os.environ[key] for key in _OS_KEYS if key in os.environ},
            "PYTHONPATH": str(REPO_ROOT / "src"),
            **EXAMPLE_ENV,
        },
        capture_output=True,
        text=True,
        timeout=120,
    )


def test_the_example_compiles_with_only_the_documented_environment(entrypoint):
    assert entrypoint.returncode == 0, entrypoint.stderr


def test_the_example_prints_the_dag_it_derived_from_the_sql(entrypoint):
    """The four stages are the whole point of the example.

    Nobody declared these edges. Both bronze tables are independent, so they
    share stage 1; the rest fall out of `bronze.Customers`, `bronze.Orders` and
    `silver.DimCustomer` appearing in the models' own SELECT statements.
    """
    assert entrypoint.stdout.splitlines() == [
        "stage 1: Customers, Orders",
        "stage 2: DimCustomer",
        "stage 3: FactOrders",
        "stage 4: vw_Revenue",
    ]
