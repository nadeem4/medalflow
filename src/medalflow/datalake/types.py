"""The shape a data lake listing comes back in.

:class:`FileInfo` is one entry from a directory listing, carrying the same
location three ways because callers need different ones -- relative to the
directory that was listed, absolute within the filesystem, and as an
``abfs://`` URL for handing to something that reads the lake directly.
Computing all three once is cheaper than making every caller reassemble the
two it did not get.
"""

from pydantic import BaseModel


class FileInfo(BaseModel):
    """Information about a file or directory in the data lake.

    Attributes:
        relative_path: Path relative to the directory parameter
        absolute_path: Full path in the data lake
        abfs_path: Full ABFS URL (abfs://container@account.dfs.core.windows.net/path)
        is_directory: True if this is a directory, False if it's a file
    """

    relative_path: str
    absolute_path: str
    abfs_path: str
    is_directory: bool
