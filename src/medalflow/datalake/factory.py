"""Factory for creating datalake clients."""

from medalflow.constants.datalake import LakeType
from medalflow.logging import get_logger

from .client import DatalakeClient

logger = get_logger(__name__)


def get_processed_datalake_client() -> DatalakeClient:
    """Get a client for the Processed lake.

    Returns:
        DatalakeClient configured for Processed lake
    """
    logger.debug(f"Creating client for {LakeType.PROCESSED.value} lake")
    return DatalakeClient(LakeType.PROCESSED)


def get_internal_datalake_client() -> DatalakeClient:
    """Get a client for the Internal lake.

    Returns:
        DatalakeClient configured for Internal lake
    """
    logger.debug(f"Creating client for {LakeType.INTERNAL.value} lake")
    return DatalakeClient(LakeType.INTERNAL)
