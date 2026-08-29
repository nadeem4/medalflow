"""The package walk every layer's discovery shares (ADR 002, Decision 6).

Discovery is the same job in every layer: import a configured package, import
each module beneath it, and keep the classes carrying that layer's metadata
attribute. Only two things differ -- which package, and which attribute -- so
both are declared by the subclass and everything else lives here.

What a subclass supplies:

* ``layer`` -- 'bronze', 'silver' or 'gold'. Names the settings key the package
  is resolved from, the cache namespace, and the layer in error messages.
* ``metadata_attribute`` -- the attribute the layer's decorator attaches, e.g.
  ``_silver_metadata``.
* ``_extract_metadata_from_class`` -- turns one decorated class into the
  layer's own metadata record, or None to drop it (a disabled model, or one
  the layer filters out). Whatever it returns must carry a ``name``.

Two things the walk guarantees for every layer, because getting either wrong
loses a model without saying so:

* A model is a class that carries the layer's metadata attribute **itself**.
  Inheriting one from a decorated base class does not make a subclass a model.
* A ``name`` identifies exactly one model. Declaring it twice raises.
"""

import importlib
import inspect
import pkgutil
from collections.abc import Generator
from typing import TYPE_CHECKING, Any

from medalflow.core.features import get_feature_manager
from medalflow.logging import get_logger
from medalflow.protocols import CacheProtocol

if TYPE_CHECKING:
    from medalflow.settings import MedalflowSettings


class PackageNotImportable(ValueError):
    """The package configured for a layer could not be imported.

    Distinct from "the package imported and declared no models", which is a
    legitimate project shape and not an error. This one means the name points
    at nothing -- almost always a typo in the variable that supplied it.

    A ``ValueError`` because every caller of discovery already guards it with
    one, and a mistyped package name should not need a new except clause to
    stop being silent.
    """


def _qualified_name(cls: type) -> str:
    """Import path an author can search for.

    Args:
        cls: The class to name

    Returns:
        ``module.QualName``
    """
    return f"{cls.__module__}.{cls.__qualname__}"


class _BaseDiscovery:
    """Finds a layer's decorated model classes by walking a Python package.

    Attributes:
        layer: Medallion layer this discovery serves
        metadata_attribute: Class attribute the layer's decorator attaches
        package: Python package walked for models
        settings: Application settings
        logger: Logger instance
        _cache_manager: Global cache manager, when the feature is enabled
    """

    layer: str = ""
    metadata_attribute: str = ""

    def __init__(
        self,
        package: str | None = None,
        settings: "MedalflowSettings | None" = None,
    ):
        """Initialize the discovery service.

        Args:
            package: Package to walk. Defaults to the one configured for this
                layer, which raises if the project has configured none.
            settings: Application settings. Resolved from the global singleton
                when not supplied.
        """
        if settings is None:
            from medalflow.settings import get_settings

            settings = get_settings()

        self.settings = settings
        self.package = package or self.settings.package_for_layer(self.layer)
        self.logger = get_logger(self.__class__.__name__)

        self._cache_manager: CacheProtocol | None = get_feature_manager("cache")

        self.logger.info(f"Initialized {self.__class__.__name__} for package: {self.package}")

    # --- public surface -----------------------------------------------------

    def discover_all(self, force_refresh: bool = False) -> list[Any]:
        """Discover every model this layer declares in its package.

        Args:
            force_refresh: Re-walk the package even if a cached result exists

        Returns:
            One metadata record per discovered model
        """
        cache_key = f"{self.layer}:metadata:all"

        if force_refresh and self._cache_manager:
            self.logger.debug(f"Force refresh requested, clearing {self.layer} metadata cache")
            self._cache_manager.clear(f"{self.layer}:metadata:*")

        if self._cache_manager and not force_refresh:
            cached_data = self._cache_manager.get(cache_key)
            if cached_data is not None:
                self.logger.debug(f"Cache hit for key: {cache_key}")
                return cached_data

        result = self._perform_discovery()

        if self._cache_manager:
            self._cache_manager.set(cache_key, result)
            self.logger.debug(f"Cached {len(result)} models with key: {cache_key}")

        return result

    def clear_cache(self, pattern: str | None = None) -> None:
        """Clear this layer's discovery cache.

        Args:
            pattern: Optional pattern to clear specific entries. Defaults to
                every entry for this layer.
        """
        if not self._cache_manager:
            self.logger.debug("Cache manager not available, skipping cache clear")
            return

        cleared = self._cache_manager.clear(pattern or f"{self.layer}:metadata:*")
        self.logger.info(f"Cleared {cleared} cache entries for the {self.layer} layer")

    def get_cache_stats(self) -> dict[str, Any]:
        """Report whether caching is active, and its global statistics.

        Returns:
            Dictionary with cache statistics
        """
        stats: dict[str, Any] = {"cache_available": self._cache_manager is not None}

        if self._cache_manager:
            stats["global_cache_stats"] = self._cache_manager.get_stats()

        return stats

    # --- the walk -----------------------------------------------------------

    def _perform_discovery(self) -> list[Any]:
        """Walk the package and collect one metadata record per model.

        Returns:
            One metadata record per discovered model

        Raises:
            PackageNotImportable: If the configured package does not import
            ValueError: If a module cannot be processed, if a decorated class
                in it cannot be read, or if two models declare the same name.
                A model that fails is an authoring error, never a reason to
                quietly shrink the plan.
        """
        self.logger.info(f"Starting discovery of {self.layer} models in {self.package}")

        discovered: dict[str, Any] = {}
        # Which class each name came from, so a collision can name both ends.
        # Kept separate from `discovered` because the base promises only that a
        # metadata record carries a `name`, not that it carries its class.
        declared_by: dict[str, type] = {}

        for module in self._walk_package():
            try:
                for cls in self._extract_model_classes(module):
                    try:
                        metadata = self._extract_metadata_from_class(cls)
                    except Exception as e:
                        self.logger.error(f"Failed to extract metadata from {cls.__name__}: {e}")
                        raise ValueError(
                            f"Failed to read {self.layer} metadata from "
                            f"{module.__name__}.{cls.__name__}: {e}"
                        ) from e

                    if metadata:
                        previous = declared_by.get(metadata.name)
                        if previous is not None:
                            # `discovered` is keyed by name, so this used to
                            # resolve to whichever class the walk reached last
                            # and the other simply vanished from the plan.
                            raise ValueError(
                                f"Two {self.layer} models are both named "
                                f"{metadata.name!r}: {_qualified_name(previous)} and "
                                f"{_qualified_name(cls)}. A model's name is its "
                                f"identity -- discovery keys on it -- so one of them "
                                f"would be left out of the plan. Rename one."
                            )

                        declared_by[metadata.name] = cls
                        discovered[metadata.name] = metadata
                        self.logger.debug(
                            f"Discovered {self.layer} model: {metadata.name} "
                            f"in {module.__name__}"
                        )

            except ValueError:
                raise
            except Exception as e:
                self.logger.error(f"Failed to process module {module.__name__}: {e}")
                raise ValueError(
                    f"Failed to process {self.layer} model module '{module.__name__}': {e}"
                ) from e

        self.logger.info(f"Discovery complete: {len(discovered)} {self.layer} models found")

        return list(discovered.values())

    def _walk_package(self) -> Generator:
        """Import every module under the configured package.

        Two failures, both of them errors. The package itself may not import,
        which used to be logged and swallowed -- so a mistyped
        ``MEDALFLOW_MODELS_PACKAGE`` produced an empty plan and no complaint,
        and a log line is not something a caller can act on. Or a module
        beneath it may fail to import, which already raised.

        What is *not* an error: a package that imports and declares no models.
        A project using two of the three layers is a legitimate shape, so
        "does not exist" and "exists and is empty" have to stay distinguishable
        -- and they are, because only the first raises ImportError.

        Yields:
            Module objects from the package

        Raises:
            PackageNotImportable: If the configured package cannot be imported
            ValueError: If a submodule exists but cannot be imported
        """
        try:
            package = importlib.import_module(self.package)
            package_path = package.__path__
        except ImportError as e:
            self.logger.error(f"Could not import {self.layer} package {self.package}: {e}")
            raise PackageNotImportable(
                f"The {self.layer} layer is configured to look for models in "
                f"'{self.package}', which cannot be imported: {e}. Check the "
                f"package name in MEDALFLOW_{self.layer.upper()}_PACKAGE, or in "
                f"MEDALFLOW_MODELS_PACKAGE (whose '{self.layer}' subpackage is "
                f"walked), and that it is importable from where MedalFlow runs. "
                f"A package that imports and declares no {self.layer} models is "
                f"fine; one that does not import is not."
            ) from e

        for _importer, modname, _ispkg in pkgutil.walk_packages(
            package_path, prefix=f"{self.package}."
        ):
            if "__pycache__" in modname or "test" in modname.lower():
                continue

            try:
                self.logger.debug(f"Importing module: {modname}")
                module = importlib.import_module(modname)
            except Exception as e:
                self.logger.error(f"Could not import {modname}: {e}")
                raise ValueError(
                    f"Failed to import {self.layer} model module '{modname}': {e}"
                ) from e

            yield module

    def _extract_model_classes(self, module) -> list[type]:
        """Find this layer's decorated classes in one module.

        Args:
            module: Python module to inspect

        Returns:
            Classes the module itself defines that carry the layer's metadata
        """
        classes = []

        for _name, obj in inspect.getmembers(module, inspect.isclass):
            # A class imported into this module belongs to the module that
            # defines it, and is discovered there.
            if obj.__module__ != module.__name__:
                continue

            if self._is_model_class(obj):
                classes.append(obj)
                self.logger.debug(f"Found {self.layer} model class: {obj.__name__}")

        return classes

    def _is_model_class(self, cls: type) -> bool:
        """Check whether a class carries this layer's decorator itself.

        The attribute has to be declared on `cls`, not merely reachable from
        it. This was `hasattr`, which is satisfied by an *inherited* attribute,
        so any subclass of a decorated model was discovered as though it had
        been decorated too. The `__module__` guard in `_extract_model_classes`
        does not catch it: a subclass genuinely belongs to its own module.

        Where a layer takes its `name` from the metadata, the subclass
        inherited the parent's name as well -- and, keyed by name, displaced
        the model it inherited from.

        Args:
            cls: Class to check

        Returns:
            True if the layer's metadata attribute is defined on this class
        """
        return self.metadata_attribute in cls.__dict__

    def _extract_metadata_from_class(self, cls: type) -> Any | None:
        """Turn one decorated class into this layer's metadata record.

        Args:
            cls: Decorated class

        Returns:
            The layer's metadata record, or None to leave the class out

        Raises:
            NotImplementedError: Always; every subclass supplies this.
        """
        raise NotImplementedError
