
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