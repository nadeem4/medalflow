"""A project whose two silver models read each other.

Both models compile fine on their own: their methods run and their operations
build. What fails is the plan -- the dependency edges their SQL implies form a
cycle, so there is no order to execute them in. `compile()` has to report that
as an error rather than raise, and still return a result.
"""
