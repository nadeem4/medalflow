"""A project whose silver models are broken in three different ways.

`compile()` collects errors rather than raising them, so an author fixing three
models learns about all three in one run. That promise needs a project with
three independently broken models and one healthy one: the healthy model must
still reach the plan.

Bronze and gold are present but empty -- a layer with no models is not an
error, and leaving them unconfigured would add package errors that have
nothing to do with what this fixture is for.
"""
