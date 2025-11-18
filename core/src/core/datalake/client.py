"""Simple Azure Data Lake Storage client."""
from dataclasses import dataclass
from typing import List, Optional, Union
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential

from core.settings import get_settings
from .types import FileInfo
from core.constants.datalake import LakeType, DataLakeAuthMethod
from core.common.exceptions import CTEError
from core.logging import get_logger
from core.utils.decorators import traced

logger = get_logger(__name__)





class DatalakeClient:
    """Simple client for Azure Data Lake Storage operations."""
    
    def __init__(self, lake_type: LakeType = LakeType.PROCESSED):
        """Initialize client for specified lake.
        
        Args:
            lake_type: Which lake to connect to (PROCESSED or INTERNAL)
            prefix_paths: Whether to add data source prefix to paths
        """
        self.settings = get_settings()
        self.lake_type = lake_type
        self.config = self.settings.datalake.get_lake_config(lake_type)
        if not self.config:
            raise ValueError(f"No configuration found for lake type: {lake_type.value}")
        
        self._service_client = None
        self._fs_client = None
        self.file_system_name = self.settings.datasource_file_system
        
    def _get_fs_client(self):
        """Get or create file system client."""
        if self._fs_client is None:
            if self._service_client is None:
                # Create credential
                if self.config.auth_method == DataLakeAuthMethod.ACCESS_KEY:
                    credential = self.config.access_key
                else:
                    credential = DefaultAzureCredential()
                
                # Create service client
                self._service_client = DataLakeServiceClient(
                    account_url=f"https://{self.config.account_name}.dfs.core.windows.net",
                    credential=credential
                )
            
    
                
            self._fs_client = self._service_client.get_file_system_client(self.file_system_name)
        return self._fs_client
    
    def _get_full_path(self, path: str) -> str:
        path = path.lstrip('/')
        return f"{self.settings.base_path}/{path}"
        
    
    def _get_abfs_path(self, path: str) -> str:
        """Construct ABFS URL for pandas operations.
        
        Args:
            path: File path (will be prefixed if configured)
            
        Returns:
            ABFS URL in format: abfs://container@account.dfs.core.windows.net/path
        """
        full_path = self._get_full_path(path)
        
        return f"abfs://{self.file_system_name}@{self.config.account_name}.dfs.core.windows.net/{full_path}"
    
    def _get_storage_options(self) -> dict:
        """Get storage options for pandas ABFS operations.
        
        Returns:
            Dictionary with authentication options for pandas
        """
        if self.config.auth_method == DataLakeAuthMethod.ACCESS_KEY:
            return {"account_key": self.config.access_key()}
        else:
            return {"anon": False, "use_azure_identity": True}

    def _span_attributes(
        self,
        *,
        operation: str,
        path: Optional[str] = None,
        recursive: Optional[bool] = None,
    ) -> dict:
        """Build telemetry attributes for storage operations."""
        attributes = {
            "storage.system": "azure.datalake",
            "storage.account": self.config.account_name,
            "storage.file_system": self.config.file_system_name,
            "storage.operation": operation,
            "medalflow.storage.lake_type": self.lake_type.value,
        }

        if path:
            attributes["storage.path"] = path
            suffix = path.split(".")[-1].lower() if "." in path else None
            if suffix:
                attributes["storage.format"] = suffix

        if recursive is not None:
            attributes["storage.recursive"] = recursive

        return attributes
    
    @traced(
        span_name="medalflow.datalake.upload",
        attribute_getter=lambda self, data, path, **kwargs: self._span_attributes(
            operation="upload",
            path=self._get_full_path(path),
        ),
    )
    def upload(self, data: Union[pd.DataFrame, bytes, str], path: str, **kwargs):
        """Upload data to the lake.
        
        Args:
            data: DataFrame, bytes, or string to upload
            path: Target path in the lake
            **kwargs: Additional arguments passed to pandas write functions
        """
        abfs_path = self._get_abfs_path(path)
        storage_options = self._get_storage_options()
        
        try:
            if path.endswith('.csv'):
                data.to_csv(abfs_path, storage_options=storage_options, index=False, **kwargs)
            elif path.endswith('.json'):
                data.to_json(abfs_path, storage_options=storage_options, orient='records', **kwargs)
            else:  # Default to parquet
                data.to_parquet(abfs_path, storage_options=storage_options, engine='pyarrow', **kwargs)
            
            logger.info(f"Uploaded DataFrame to {self.lake_type.value}: {path}")
        except Exception as e:
            logger.error(f"Failed to upload DataFrame to {self.lake_type.value}: {path}", exc_info=True)
            raise CTEError(f"Failed to upload to {path}: {e}") from e
    
    @traced(
        span_name="medalflow.datalake.read",
        attribute_getter=lambda self, path, **kwargs: self._span_attributes(
            operation="read",
            path=path if path.startswith('abfs://') else self._get_full_path(path),
        ),
    )
    def read(self, path: str, **kwargs) -> Union[pd.DataFrame, bytes]:
        """Read file from the lake.
        
        Args:
            path: Path to read from (can be relative path or full ABFS URL)
            **kwargs: Additional arguments passed to pandas read functions
            
        Returns:
            DataFrame for parquet/csv/json files, bytes for others
        """
        # Check if it's already an ABFS path
        if path.startswith('abfs://'):
            abfs_path = path
        else:
            abfs_path = self._get_abfs_path(path)
        storage_options = self._get_storage_options()
        
        try:
            if path.endswith('.parquet'):
                return pd.read_parquet(abfs_path, storage_options=storage_options, **kwargs)
            elif path.endswith('.csv'):
                return pd.read_csv(abfs_path, storage_options=storage_options, **kwargs)
            elif path.endswith('.json'):
                return pd.read_json(abfs_path, storage_options=storage_options, **kwargs)
        except Exception as e:
            logger.error(f"Failed to read DataFrame from {self.lake_type.value}: {path}", exc_info=True)
            raise CTEError(f"Failed to read {path}: {e}") from e
    
    @traced(
        span_name="medalflow.datalake.delete",
        attribute_getter=lambda self, path: self._span_attributes(
            operation="delete",
            path=self._get_full_path(path),
        ),
    )
    def delete(self, path: str):
        """Delete a file or directory (no error if doesn't exist)."""
        if not self.exists(path):
            logger.debug(f"Path doesn't exist, nothing to delete: {path}")
            return
        
        full_path = self._get_full_path(path)
        
        try:
            file_client = self._get_fs_client().get_file_client(full_path)
            file_client.delete_file()
            logger.info(f"Deleted file from {self.lake_type.value}: {full_path}")
        except:
            try:
                dir_client = self._get_fs_client().get_directory_client(full_path)
                dir_client.delete_directory()
                logger.info(f"Deleted directory from {self.lake_type.value}: {full_path}")
            except Exception as e:
                logger.error(f"Failed to delete from {self.lake_type.value}: {full_path}", exc_info=True)
                raise CTEError(f"Failed to delete {path}: {e}") from e
    
    @traced(
        span_name="medalflow.datalake.exists",
        attribute_getter=lambda self, path: self._span_attributes(
            operation="exists",
            path=self._get_full_path(path),
        ),
    )
    def exists(self, path: str) -> bool:
        """Check if path exists.
        
        Args:
            path: Path to check
            
        Returns:
            True if path exists, False otherwise
        """
        full_path = self._get_full_path(path)
        
        try:
            file_client = self._get_fs_client().get_file_client(full_path)
            file_client.get_file_properties()
            return True
        except:
            try:
                dir_client = self._get_fs_client().get_directory_client(full_path)
                dir_client.get_directory_properties()
                return True
            except:
                return False
    
    @traced(
        span_name="medalflow.datalake.list_files",
        attribute_getter=lambda self, directory="", recursive=False: self._span_attributes(
            operation="list_files",
            path=self._get_full_path(directory or ""),
            recursive=recursive,
        ),
    )
    def list_files(self, directory: str = "", recursive: bool = False) -> List[FileInfo]:
        """List files in directory.
        
        Args:
            directory: Directory path (empty string for root)
            recursive: Whether to list recursively
            
        Returns:
            List of FileInfo objects containing file/directory information
        """
        full_path = self._get_full_path(directory) if directory else ""
        
        
        try:
            paths = self._get_fs_client().get_paths(
                path=full_path, 
                recursive=recursive
            )
            
            # Build FileInfo objects with all path types
            result = []
            prefix_to_remove = full_path + "/" if full_path else ""
            for path in paths:
                # Calculate relative path
                if path.name.startswith(prefix_to_remove):
                    relative_path = path.name[len(prefix_to_remove):]
                else:
                    relative_path = path.name
                
                # Construct ABFS path
                abfs_path = f"abfs://{self.file_system_name}@{self.config.account_name}.dfs.core.windows.net/{path.name}"
                
                # Create FileInfo object
                file_info = FileInfo(
                    relative_path=relative_path,
                    absolute_path=path.name,
                    abfs_path=abfs_path,
                    is_directory=path.is_directory
                )
                    
                # Only include files, not directories (unless recursive)
                if not path.is_directory or recursive:
                    result.append(file_info)
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to list {directory} in {self.lake_type.value}", exc_info=True)
            raise CTEError(f"Failed to list {directory}: {e}") from e
    
    def create_directory(self, directory: str):
        """Create a directory.
        
        Args:
            directory: Directory path to create
        """
        full_path = self._get_full_path(directory)
        
        try:
            dir_client = self._get_fs_client().get_directory_client(full_path)
            dir_client.create_directory()
            logger.info(f"Created directory in {self.lake_type.value}: {full_path}")
        except Exception as e:
            logger.error(f"Failed to create directory in {self.lake_type.value}: {full_path}", exc_info=True)
            raise CTEError(f"Failed to create directory {directory}: {e}") from e
    
    # Convenience methods for specific formats
    
    def upload_parquet(self, df: pd.DataFrame, path: str, **kwargs):
        """Upload DataFrame as Parquet file.
        
        Args:
            df: DataFrame to upload
            path: Target path (will add .parquet if missing)
            **kwargs: Additional arguments passed to to_parquet
        """
        if not path.endswith('.parquet'):
            path += '.parquet'
        
        # Use the main upload method which now uses pandas directly
        self.upload(df, path, **kwargs)
    
    def upload_csv(self, df: pd.DataFrame, path: str, **kwargs):
        """Upload DataFrame as CSV file.
        
        Args:
            df: DataFrame to upload
            path: Target path (will add .csv if missing)
            **kwargs: Additional arguments passed to to_csv
        """
        if not path.endswith('.csv'):
            path += '.csv'
        
        # Use the main upload method which now uses pandas directly
        self.upload(df, path, **kwargs)
    
    def upload_json(self, df: pd.DataFrame, path: str, **kwargs):
        """Upload DataFrame as JSON file.
        
        Args:
            df: DataFrame to upload
            path: Target path (will add .json if missing)
            **kwargs: Additional arguments passed to to_json
        """
        if not path.endswith('.json'):
            path += '.json'
        
        # Use the main upload method which now uses pandas directly
        self.upload(df, path, **kwargs)
    
    def read_parquet(self, path: str, **kwargs) -> pd.DataFrame:
        """Read Parquet file as DataFrame.
        
        Args:
            path: Path to Parquet file
            **kwargs: Additional arguments passed to pd.read_parquet
            
        Returns:
            DataFrame
        """
        if not path.endswith('.parquet'):
            path += '.parquet'
        # Use the main read method which now uses pandas directly
        return self.read(path, **kwargs)
    
    def read_csv(self, path: str, **kwargs) -> pd.DataFrame:
        """Read CSV file as DataFrame.
        
        Args:
            path: Path to CSV file
            **kwargs: Additional arguments passed to pd.read_csv
            
        Returns:
            DataFrame
        """
        if not path.endswith('.csv'):
            path += '.csv'
        # Use the main read method which now uses pandas directly
        return self.read(path, **kwargs)
    
    def read_json(self, path: str, **kwargs) -> pd.DataFrame:
        """Read JSON file as DataFrame.
        
        Args:
            path: Path to JSON file (can be relative path or full ABFS URL)
            **kwargs: Additional arguments passed to pd.read_json
            
        Returns:
            DataFrame
        """
        # Only add .json extension if it's not an ABFS path and doesn't already have it
        if not path.startswith('abfs://') and not path.endswith('.json'):
            path += '.json'
        # Use the main read method which now uses pandas directly
        return self.read(path, **kwargs)
    
    def close(self):
        """Close the client and release resources."""
        self._fs_client = None
        self._service_client = None
