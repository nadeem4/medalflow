"""Dynamic metadata discovery for Silver layer transformations.

This module provides dynamic discovery of all silver transformations by
importing Python modules and extracting decorator metadata, eliminating
the need for external configuration files (CSV, JSON).
"""

import importlib
import inspect
import pkgutil
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from medalflow.core.features import get_feature_manager
from medalflow.logging import get_logger
from medalflow.protocols import CacheProtocol
from medalflow.settings import get_settings
from medalflow.types import SilverMetadata

from .sequencer import SilverTransformationSequencer


@dataclass
class TransformationMetadata:
    """Metadata for a discovered silver transformation.

    Only contains essential fields. Other fields are accessed via properties
    from the stored silver_metadata object.
    """

    sp_name: str
    model_name: str
    sequencer_class: type[SilverTransformationSequencer]
    silver_metadata: SilverMetadata

    @property
    def description(self) -> str:
        """Get description from silver metadata."""
        return self.silver_metadata.description or ""

    @property
    def tags(self) -> list[str]:
        """Get tags from silver metadata."""
        return self.silver_metadata.tags or []

    @property
    def group_file_name(self) -> str:
        """Get group file name from silver metadata."""
        return self.silver_metadata.group_file_name

    @property
    def silver_table_name(self) -> str:
        """Compute silver table name from sp_name."""
        # removeprefix, not replace: only a leading "Load_" is the marker, so
        # `Load_Load_Order` is `Load_Order`, not `Order`.
        return self.sp_name.removeprefix("Load_")

    @property
    def module_path(self) -> str:
        """Get full module path of the sequencer class."""
        return f"{self.sequencer_class.__module__}.{self.sequencer_class.__name__}"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary including computed properties."""
        return {
            "sp_name": self.sp_name,
            "model_name": self.model_name,
            "silver_table_name": self.silver_table_name,
            "group_file_name": self.group_file_name,
            "description": self.description,
            "tags": self.tags,
            "module_path": self.module_path,
            "sequencer_class_name": self.sequencer_class.__name__,
        }


class SilverMetadataDiscovery:
    """Dynamically discovers all silver transformations from Python modules.

    This service eliminates the need for external configuration files by
    discovering transformations directly from decorated Python classes in
    the silver package. Uses a global cache manager for performance optimization
    when available.

    Attributes:
        silver_package: Package name for silver transformations
        logger: Logger instance
        _cache_manager: Global cache manager for caching metadata
    """

    def __init__(self, silver_package: str | None = None):
        """Initialize the discovery service.

        Args:
            silver_package: Optional package name override
        """
        self.settings = get_settings()
        self.silver_package = silver_package or self.settings.silver_package_name
        self.logger = get_logger(self.__class__.__name__)

        # Get the global cache manager if available
        self._cache_manager: CacheProtocol | None = get_feature_manager("cache")

        self.logger.info(f"Initialized SilverMetadataDiscovery for package: {self.silver_package}")
        if self._cache_manager:
            self.logger.debug("Global cache manager is available for silver metadata")
        else:
            self.logger.debug("Cache manager not available - will perform discovery on each call")

    def discover_all_transformations(
        self, force_refresh: bool = False
    ) -> list[TransformationMetadata]:
        """Discover all silver transformations from the package.

        This method walks the entire silver package tree, imports each module,
        finds classes with decorators, and extracts their metadata.

        Uses global cache manager for performance when available.

        Args:
            force_refresh: Force re-discovery even if cache exists

        Returns:
            List of discovered transformation metadata
        """
        cache_key = "silver:metadata:all"

        # If force refresh, clear related caches
        if force_refresh and self._cache_manager:
            self.logger.debug("Force refresh requested, clearing silver metadata cache")
            self._cache_manager.clear("silver:metadata:*")

        # Check cache first if available
        if self._cache_manager and not force_refresh:
            cached_data = self._cache_manager.get(cache_key)
            if cached_data is not None:
                self.logger.debug(f"Cache hit for key: {cache_key}")
                return cached_data

        # Perform actual discovery
        result = self._perform_discovery()

        # Cache the results if cache manager is available
        if self._cache_manager:
            self._cache_manager.set(cache_key, result)
            self.logger.debug(f"Cached {len(result)} transformations with key: {cache_key}")

        return result

    def _perform_discovery(self) -> list[TransformationMetadata]:
        """Perform the actual discovery of transformations.

        Internal method that does the actual work of discovering transformations
        by walking the package tree and extracting metadata.

        Returns:
            List of discovered transformation metadata
        """
        self.logger.info(f"Starting discovery of transformations in {self.silver_package}")

        metadata_dict: dict[str, TransformationMetadata] = {}
        discovered_count = 0
        error_count = 0

        for module in self._walk_silver_package():
            try:
                classes = self._extract_transformation_classes(module)

                for cls in classes:
                    try:
                        metadata = self._extract_metadata_from_class(cls)
                        if metadata:
                            metadata_dict[metadata.sp_name] = metadata
                            discovered_count += 1
                            self.logger.debug(
                                f"Discovered transformation: {metadata.sp_name} "
                                f"[{metadata.model_name}] in {metadata.module_path}"
                            )
                    except Exception as e:
                        self.logger.error(f"Failed to extract metadata from {cls.__name__}: {e}")
                        raise ValueError(
                            f"Failed to read silver metadata from "
                            f"{module.__name__}.{cls.__name__}: {e}"
                        ) from e

            except ValueError:
                raise
            except Exception as e:
                self.logger.error(f"Failed to process module {module.__name__}: {e}")
                raise ValueError(
                    f"Failed to process silver model module " f"'{module.__name__}': {e}"
                ) from e

        self.logger.info(
            f"Discovery complete: {discovered_count} transformations found, "
            f"{error_count} errors encountered"
        )

        return list(metadata_dict.values())

    def get_transformations_by_models(self, models: str) -> list[TransformationMetadata]:
        """Get all transformations for a specific model.

        Uses cache for improved performance when available.

        Args:
            model: Model name to filter by

        Returns:
            List of transformations for the specified model
        """
        if models.lower() == "all":
            return self.discover_all_transformations()

        models = [model.strip().lower() for model in models.strip().split(",")]
        result = []
        all_transformations = self.discover_all_transformations()

        result = [
            metadata for metadata in all_transformations if metadata.model_name.lower() in models
        ]

        self.logger.debug(f"Found {len(result)} transformations for models: {models}")

        return result

    def get_transformation_by_sp(self, sp_names: str) -> list[TransformationMetadata]:
        """Get transformations by stored procedure name.

        Uses cache for improved performance when available.

        Args:
            sp_names: Comma-separated stored procedure names

        Returns:
            Every transformation whose sp_name matches, empty if none do
        """

        sp_names = [name.strip().lower() for name in sp_names.strip().split(",")]

        all_transformations = self.discover_all_transformations()

        result = [
            metadata for metadata in all_transformations if metadata.sp_name.lower() in sp_names
        ]

        self.logger.debug(f"Found {len(result)} transformations for SPs: {sp_names}")

        return result

    def _walk_silver_package(self) -> Generator:
        """Walk all modules in the silver package.

        Yields:
            Module objects from the silver package
        """
        try:
            package = importlib.import_module(self.silver_package)
            package_path = package.__path__
        except ImportError as e:
            self.logger.error(f"Could not import silver package {self.silver_package}: {e}")
            return

        for _importer, modname, _ispkg in pkgutil.walk_packages(
            package_path, prefix=f"{self.silver_package}."
        ):
            if "__pycache__" in modname or "test" in modname.lower():
                continue

            try:
                self.logger.debug(f"Importing module: {modname}")
                module = importlib.import_module(modname)
            except Exception as e:
                self.logger.error(f"Could not import {modname}: {e}")
                raise ValueError(f"Failed to import silver model module '{modname}': {e}") from e

            yield module

    def _extract_transformation_classes(self, module) -> list[type]:
        """Extract all transformation classes from a module.

        Args:
            module: Python module to inspect

        Returns:
            List of classes that are transformations
        """
        classes = []

        for _name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ != module.__name__:
                continue

            if self._is_transformation_class(obj):
                classes.append(obj)
                self.logger.debug(
                    f"Found transformation class: {obj.__name__} in {module.__name__}"
                )

        return classes

    def _is_transformation_class(self, cls: type) -> bool:
        """Check if a class is a silver transformation.

        Args:
            cls: Class to check

        Returns:
            True if the class has silver_metadata decorator
        """
        return hasattr(cls, "_silver_metadata")

    def _extract_metadata_from_class(self, cls: type) -> TransformationMetadata | None:
        """Extract and normalize metadata from decorated class.

        Only returns metadata for enabled transformations.

        Args:
            cls: Decorated class

        Returns:
            TransformationMetadata or None if extraction fails or transformation is disabled
        """
        try:
            if not hasattr(cls, "_silver_metadata"):
                return None

            meta: SilverMetadata = cls._silver_metadata

            model_name = meta.model_name or self._extract_model_from_group(meta.group_file_name)

            if meta.disabled:
                self.logger.debug(f"Skipping disabled transformation: {meta.sp_name}")
                return None

            if not self.settings.is_model_configured(model_name):
                self.logger.debug(
                    f"Skipping transformation {meta.sp_name}: model '{model_name}' not configured"
                )
                return None

            return TransformationMetadata(
                sp_name=meta.sp_name,
                model_name=model_name,
                sequencer_class=cls,
                silver_metadata=meta,
            )

        except Exception as e:
            # Surfaced to the caller: a model whose metadata cannot be read is
            # an authoring error, not a reason to drop it from the plan.
            self.logger.error(f"Failed to extract metadata from {cls.__name__}: {e}")
            raise

    def _extract_model_from_group(self, group_file_name: str) -> str:
        """Extract model name from group_file_name.

        Args:
            group_file_name: Group file path (e.g., 'group_sales/parallel_group_1.json')

        Returns:
            Model name (e.g., 'sales')
        """
        if not group_file_name:
            raise ValueError("group_file_name is required to extract model name")

        if "/" not in group_file_name:
            raise ValueError("group_file_name must contain a '/' to extract model name")

        # format: 'group_sales/parallel_group_1.json'
        group = group_file_name.split("/")[0]
        return group.replace("group_", "")

    def clear_cache(self, pattern: str | None = None) -> None:
        """Clear silver metadata cache.

        Args:
            pattern: Optional pattern to clear specific cache entries.
                     If None, clears all silver metadata cache.
                     Examples: 'silver:metadata:model:sales', 'silver:metadata:sp:*'
        """
        if not self._cache_manager:
            self.logger.debug("Cache manager not available, skipping cache clear")
            return

        if pattern:
            cleared = self._cache_manager.clear(pattern)
            self.logger.info(f"Cleared {cleared} cache entries matching pattern: {pattern}")
        else:
            # Clear all silver metadata
            cleared = self._cache_manager.clear("silver:metadata:*")
            self.logger.info(f"Cleared {cleared} silver metadata cache entries")

    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics for silver metadata.

        Returns:
            Dictionary with cache statistics
        """
        stats = {"cache_available": self._cache_manager is not None}

        if self._cache_manager:
            # Get global cache stats
            global_stats = self._cache_manager.get_stats()
            stats["global_cache_stats"] = global_stats

        return stats
