"""The layer that actually talks to the database.

One class, :class:`~medalflow.compute.engines.base.BaseSQLEngine`, and one
subclass of it per platform -- currently only
:class:`~medalflow.compute.engines.synapse.SynapseSQLEngine`. It is a concrete
class, not an abstract one: a subclass that overrides nothing is a working
engine, and the two hooks it offers (``_apply_connection_settings`` and
``get_connection_info``) both have working defaults.

An engine takes finished SQL and runs it. It owns the SQLAlchemy engine and its
connection pool, the retry-with-backoff around each call, and the four ways a
result comes back -- nothing (``execute_query``), a DataFrame, a scalar, or a
list of row mappings. ``execute_batch`` runs several statements on one
connection.

What an engine does *not* do, because something else does:

* **Generate SQL** -- :mod:`medalflow.query_builder`. That is also where
  identifier validation lives: an engine is handed a string and runs it, so it
  is not the layer that makes the string safe.
* **Choose a platform** -- :mod:`medalflow.compute.platforms`, which builds the
  engine and is the only thing that holds one. Engines are not part of the
  public API and nothing outside the compute module constructs one.

Everything the engine reads is loaded whole: ``fetch_dataframe`` goes through
``pandas.read_sql`` and ``fetch_all`` through ``.all()``. There is no streaming
path, so a query is bounded by the memory of the process running it.

``pyodbc`` and ``pandas`` are imported inside the methods that need them --
both ship with the optional ``azure`` extra, and importing this package must
not require it.
"""

from medalflow.compute.engines.base import BaseSQLEngine

__all__ = ["BaseSQLEngine"]
