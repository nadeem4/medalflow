"""Data lake configuration loading service.

This module provides the DataLakeConfigurationService which injects
data loading capabilities into feature managers via dependency injection.
"""

import logging

from medalflow.core.features import get_feature_manager
from medalflow.datalake import get_internal_datalake_client
from medalflow.logging import get_logger

logger = logging.getLogger(__name__)


class DataLakeConfigurationService:
    """Service for injecting data lake access into feature managers.

    This service is responsible for:
    - Providing data loading capabilities to Layer 1 managers
    - Injecting CSV and JSON loaders via dependency injection
    - Maintaining clean architecture without circular dependencies

    It acts as the bridge between Layer 2 (data lake access) and
    Layer 1 (feature managers), using dependency injection to avoid
    circular dependencies.

    Example:
        >>> service = get_configuration_service()
        >>> service.initialize()  # Injects loaders into managers
        >>> # Now managers can load data from data lake
    """

    _instance = None

    def __new__(cls):
        """Ensure singleton pattern for configuration service."""
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize the configuration service."""
        # Only initialize once (singleton pattern)
        if hasattr(self, "_initialized"):
            return

        self.logger = get_logger(self.__class__.__name__)
        self.client = get_internal_datalake_client()
        self._initialized = False
        self._injected_managers = []

    def initialize(self) -> bool:
        """Initialize and inject dependencies into all managers.

        This method injects data loading functions into all feature managers
        that need them, enabling them to load data from the data lake without
        creating circular dependencies.

        Returns:
            True if initialization successful, False otherwise
        """
        if self._initialized:
            self.logger.debug("Configuration service already initialized")
            return True

        try:
            # Inject into all managers that need CSV loading
            csv_managers = ["stats"]

            for manager_name in csv_managers:
                mgr = get_feature_manager(manager_name)
                if mgr and hasattr(mgr, "set_csv_loader"):
                    mgr.set_csv_loader(self.client.read_csv)
                    self._injected_managers.append(f"{manager_name}:csv")
                    self.logger.info(f"Injected CSV loader into {manager_name} manager")
                elif mgr:
                    self.logger.debug(f"{manager_name} manager doesn't have set_csv_loader")
                else:
                    self.logger.debug(f"{manager_name} manager not available")

            # No longer need to inject JSON loader for silver grouping (removed)

            self._initialized = True
            self.logger.info(
                f"Configuration service initialized with {len(self._injected_managers)} injections"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize configuration service: {e}")
            return False


# Singleton accessor
def get_configuration_service() -> DataLakeConfigurationService:
    """Get the singleton DataLakeConfigurationService instance.

    Returns:
        The DataLakeConfigurationService singleton instance

    Example:
        >>> service = get_configuration_service()
        >>> service.initialize()
    """
    return DataLakeConfigurationService()
