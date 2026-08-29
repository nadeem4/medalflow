"""The models of a small MedalFlow project: bronze, silver and gold.

Each layer is a subpackage of this one, which is the shape
``MEDALFLOW_MODELS_PACKAGE`` expects: point it at ``models`` and MedalFlow
imports ``models.bronze``, ``models.silver`` and ``models.gold`` and collects
the decorated classes it finds.

Nothing here imports MedalFlow's test helpers, so discovery walks this package
exactly as it would walk yours. The end-to-end suite runs against these very
files, so if the example breaks, CI goes red.
"""
