"""Import smoke tests.

These mirror the CI `import-smoke` job. They exist to keep the package
importable: the 8 orphaned processor/validator stubs that imported the deleted
`core.medallion.base.processor` / `base.validator` modules broke every import
of `core`, including unrelated tests that only touch `core.logging`.
"""


def test_core_package_imports():
    import core  # noqa: F401


def test_core_medallion_imports():
    import core.medallion  # noqa: F401


def test_core_api_imports():
    import core.api  # noqa: F401
