"""Azure Data Lake Storage client for MedalFlow."""
from .factory import get_internal_datalake_client, get_processed_datalake_client

__all__ = ['get_processed_datalake_client', 'get_internal_datalake_client']
