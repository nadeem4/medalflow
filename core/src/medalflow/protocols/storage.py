"""Storage protocol definition.

The storage seam is deliberately two methods wide, because two is what the
framework actually calls on a storage client:

- ``delete(path)`` -- ``compute/platforms/synapse.py`` clears an external
  table's output location before recreating it.
- ``read_csv(path)`` -- ``datalake/services/configuration_service.py`` hands
  the bound method to ``StatsManager.set_csv_loader``.

``DatalakeClient`` exposes eleven more public methods; none has a caller in the
package. Declaring them here would widen the contract every alternative
implementation has to honour, and would drag pandas into Layer 0, for no
consumer.

``read_csv``'s return type is annotated lazily so importing this module does
not import pandas. A narrower seam already exists for the same data:
``StatsManager.set_csv_loader`` takes a plain ``Callable[[str], pd.DataFrame]``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    import pandas as pd


@runtime_checkable
class StorageClient(Protocol):
    """The object-storage operations MedalFlow depends on.

    ``DatalakeClient`` (Azure Data Lake Storage Gen2) is the implementation
    that ships with the package.
    """

    def delete(self, path: str) -> None:
        """Delete a file or directory.

        A path that does not exist is not an error.

        Args:
            path: Path within the configured storage location
        """
        ...

    def read_csv(self, path: str, **kwargs: Any) -> pd.DataFrame:
        """Read a CSV file into a DataFrame.

        Args:
            path: Path within the configured storage location
            **kwargs: Passed through to the underlying CSV reader

        Returns:
            The parsed CSV as a DataFrame
        """
        ...
