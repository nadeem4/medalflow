"""Import smoke tests.

These mirror the CI `import-smoke` job. They exist to keep the package
importable: the 8 orphaned processor/validator stubs that imported the deleted
`medalflow.medallion.base.processor` / `base.validator` modules broke every import
of `medalflow`, including unrelated tests that only touch `medalflow.logging`.
"""


def test_core_package_imports():
    import medalflow  # noqa: F401


def test_core_medallion_imports():
    import medalflow.medallion  # noqa: F401


def test_core_api_imports():
    import medalflow.api  # noqa: F401
