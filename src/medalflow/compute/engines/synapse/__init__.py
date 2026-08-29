"""Azure Synapse Analytics SQL engine.

One engine, :class:`SynapseSQLEngine`. It subclasses
:class:`~medalflow.compute.engines.base.BaseSQLEngine` and adds exactly two
things: the ``SET ARITHABORT`` / ``SET ANSI_*`` options Synapse wants on every
connection, and the lake database and external data source names in
``get_connection_info``. The ODBC connection, the pool, the retries and all
four fetch paths are inherited unchanged.

Engines are built and held by the platform (:mod:`medalflow.compute.platforms`).
Nothing outside the compute module constructs one.
"""

from medalflow.compute.engines.synapse.sql_engine import SynapseSQLEngine

__all__ = ["SynapseSQLEngine"]
