import inspect
from abc import ABC
from typing import TYPE_CHECKING, Any

from medalflow.constants.sql import QueryType
from medalflow.logging import get_logger
from medalflow.medallion.types import ExecutionPlan
from medalflow.observability.context import sanitize_extras
from medalflow.operations import BaseOperation, OperationBuilder
from medalflow.protocols.features import CacheProtocol, StatsProtocol
from medalflow.types.metadata import (
    DiscoveredMethod,
    QueryMetadata,
)

if TYPE_CHECKING:
    from medalflow.settings import MedalflowSettings


# B024: ABC here marks the class as not-for-direct-instantiation. Every method
# has a working default, so there is genuinely nothing to mark @abstractmethod;
# adding one would invent a contract the layer sequencers do not currently have.
class _BaseSequencer(ABC):  # noqa: B024
    """Turns one model's decorated methods into an execution plan.

    Every layer sequencer inherits from this. It finds the
    ``@query_metadata`` methods on the model, calls each for its SQL, builds
    the operations, and hands them to
    :class:`~medalflow.medallion.orchestration.execution_orchestrator.ExecutionPlanOrchestrator`
    -- which is where dependency analysis and stage ordering actually happen.
    This class coordinates; it does not analyse SQL itself.

    Attributes:
        logger: Structured logger, named for the concrete sequencer class
        settings: Application settings
        selection: Target table names this sequencer is restricted to, or
            None for all of them
        table_prefix: Prefix applied to generated table names
        sql_dialect: Parsing dialect, from ``compute.active_config.dialect``
        _stats_manager: Statistics feature manager, or None when disabled
        _cache_manager: Cache feature manager, or None when disabled

    A layer subclasses this and overrides very little: its layer name, the
    class-level metadata attribute its decorator sets, and any rewriting it
    does to a query on the way through. Authors do not subclass it directly
    -- they subclass :class:`~medalflow.medallion.bronze.sequencer.BronzeSequencer`,
    :class:`~medalflow.medallion.silver.sequencer.SilverTransformationSequencer`
    or :class:`~medalflow.medallion.gold.sequencer.GoldSequencer`.
    """

    def __init__(self, settings: "MedalflowSettings", selection: list[str] | None = None):
        """Initialize the base sequencer.

        Args:
            settings: Configuration settings for the sequencer
            selection: Optional list of target table names to restrict this
                sequencer to. None means every table it declares; an empty list
                means none. Applied by `_get_queries`.
        """
        self.logger = get_logger(self.__class__.__name__)

        self.settings = settings
        self.selection = selection
        self.table_prefix = self.settings.table_prefix
        self.sql_dialect = self.settings.compute.active_config.dialect

        self._stats_manager = None
        self._cache_manager = None
        self._init_feature_managers()

    def get_execution_plan(self) -> ExecutionPlan:
        """Get the execution plan for this sequencer with optional caching.

        This method is the core functionality of the sequencer. It discovers all
        methods decorated with the appropriate metadata, extracts their configuration,
        and organizes them into an execution plan. Results are cached by default
        using the cache manager if available.

        Returns:
            Dict[str, Any]: A dictionary containing the complete execution plan

        Example:
            >>> sequencer = CustomerETL()
            >>> plan = sequencer.get_execution_plan()
            >>> plan2 = sequencer.get_execution_plan()  # Returns cached result
        """
        if not self._cache_manager:
            return self._generate_execution_plan()

        cache_key = f"{self.get_layer_name()}:execution_plan:{self.get_obj_name()}"

        return self._cache_manager.get(cache_key, loader=lambda: self._generate_execution_plan())

    def _generate_execution_plan(self) -> ExecutionPlan:
        """Generate execution plan using the operation-based orchestrator.

        This method uses the ExecutionPlanOrchestrator to create
        execution plans from operations rather than discovered methods.
        """
        from medalflow.medallion.orchestration import ExecutionPlanOrchestrator

        # Get operations from this sequencer
        operations = self.get_queries()

        if not operations:
            self.logger.warning(
                "sequencer.no_operations",
                extra=sanitize_extras({"sequencer": self.__class__.__name__}),
            )
            # Return empty plan
            return ExecutionPlan(
                sequencer_name=self.__class__.__name__,
                metadata=self._get_class_metadata(),
                stages=[],
                dependency_graph={},
                lineage=None,
                total_queries=0,
            )

        # Create orchestrator and generate plan
        orchestrator = ExecutionPlanOrchestrator(self.settings)

        return orchestrator.create_execution_plan(
            operations=operations,
            metadata=self._get_class_metadata(),
            sequencer_name=self.__class__.__name__,
        )

    def _discover_methods(self) -> list[DiscoveredMethod]:
        """Discover all methods with the appropriate metadata and execute them once.

        Executes each method once to get its SQL query. For filter-based dimensions
        that return None, auto-generates an enum query.

        Returns:
            List[DiscoveredMethod]: List of discovered methods with metadata and SQL

        Raises:
            RuntimeError: If a decorated method cannot be executed
            TypeError: If a method returns an invalid type
        """
        methods = []
        skipped_methods = []

        for name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            if name.startswith("_"):
                continue

            if hasattr(method, "_query_metadata"):
                metadata: QueryMetadata = method._query_metadata

                try:
                    result = method()
                except TypeError as e:
                    if "missing" in str(e) and "required positional argument" in str(e):
                        raise RuntimeError(
                            f"Method '{name}' in {self.__class__.__name__} requires parameters. "
                            f"Methods decorated with @query_metadata must be "
                            f"callable without arguments. Consider using default parameter values "
                            f"or accessing instance attributes instead. Original error: {str(e)}"
                        ) from e
                    raise
                except AttributeError as e:
                    raise RuntimeError(
                        f"Method '{name}' in {self.__class__.__name__} failed with AttributeError. "
                        f"This usually means the method is trying to access an instance attribute "
                        f"that doesn't exist. Make sure all required attributes are initialized "
                        f"in __init__ or have default values. Original error: {str(e)}"
                    ) from e
                except Exception as e:
                    raise RuntimeError(
                        f"Method '{name}' in {self.__class__.__name__} failed during discovery. "
                        f"Error type: {type(e).__name__}. "
                        f"Error message: {str(e)}. "
                        f"Methods with @query_metadata decorator must be "
                        f"executable at discovery time."
                    ) from e

                if result is None:
                    result = self._handle_null_result(metadata)
                    if result:
                        self.logger.debug(
                            "sequencer.auto_query_generated",
                            extra=sanitize_extras(
                                {
                                    "method": name,
                                    "sequencer": self.__class__.__name__,
                                }
                            ),
                        )

                if result is None or (isinstance(result, str) and not result.strip()):
                    self.logger.info(
                        "sequencer.method_skipped_no_sql",
                        extra=sanitize_extras(
                            {
                                "method": name,
                                "sequencer": self.__class__.__name__,
                            }
                        ),
                    )
                    skipped_methods.append(name)
                    continue

                if not isinstance(result, str):
                    raise TypeError(
                        f"Method '{name}' in {self.__class__.__name__} must return either:\n"
                        f"  1. A string containing a SQL query\n"
                        f"  2. None (for filter-based dimensions with auto-generation)\n"
                        f"Instead got: {type(result).__name__}"
                    )

                result, metadata = self._transform_query_result(result, metadata)

                methods.append(DiscoveredMethod(name, method, metadata, result))
                self.logger.debug(
                    "sequencer.method_discovered",
                    extra=sanitize_extras(
                        {
                            "method": name,
                            "metadata_type": "_query_metadata",
                            "sequencer": self.__class__.__name__,
                        }
                    ),
                )

        if skipped_methods:
            self.logger.info(
                "sequencer.discovery_complete",
                extra=sanitize_extras(
                    {
                        "sequencer": self.__class__.__name__,
                        "discovered_count": len(methods),
                        "skipped_count": len(skipped_methods),
                        "skipped_methods": skipped_methods,
                    }
                ),
            )

        return methods

    def _handle_null_result(self, metadata: QueryMetadata) -> str | None:
        """Hook for subclasses to handle methods that return None.

        This allows subclasses to auto-generate queries for specific patterns,
        such as filter-based dimensions in the Silver layer.

        Args:
            metadata: Query metadata from the decorator

        Returns:
            Optional[str]: Generated SQL query or None to skip the method
        """
        return None

    def _transform_query_result(
        self, sql: str, metadata: QueryMetadata
    ) -> tuple[str, QueryMetadata]:
        """Hook for subclasses to transform SQL queries after generation.

        This method is called after a query is generated but before it's added
        to the discovered methods list. Subclasses can override this to apply
        layer-specific transformations.

        Args:
            sql: The SQL query string generated by the method
            metadata: Query metadata from the decorator

        Returns:
            str: Transformed SQL query (default: returns unchanged)
        """
        return sql, metadata

    def _get_method_source(self, method_name: str) -> str:
        """Get the class name where a method is actually defined.

        Args:
            method_name: Name of the method

        Returns:
            str: Name of the class where the method is defined
        """
        for cls in inspect.getmro(self.__class__):
            if method_name in cls.__dict__:
                return cls.__name__

        return self.__class__.__name__

    def _get_class_metadata(self) -> dict[str, Any]:
        """Get class-level metadata if available.

        Returns:
            Dict[str, Any]: Dictionary containing class metadata, or empty dict
        """
        class_metadata_attr = self._get_class_metadata_attribute()

        if class_metadata_attr and hasattr(self.__class__, class_metadata_attr):
            metadata = getattr(self.__class__, class_metadata_attr)
            if hasattr(metadata, "model_dump"):
                return metadata.model_dump()
            elif isinstance(metadata, dict):
                return metadata
            else:
                return {"metadata": metadata}

        return {}

    def _get_class_metadata_attribute(self) -> str | None:
        """Get the expected class-level metadata attribute name.

        Returns:
            Optional[str]: Attribute name for class metadata, or None
        """
        # Default implementation - subclasses should override if needed
        return None

    def _init_feature_managers(self) -> None:
        """Initialize feature managers for stats and cache.

        This method initializes the StatsManager and CacheManager,
        and ensures the data lake configuration service is set up to inject
        data loaders into the managers that need them.
        """
        from medalflow.core.features import get_feature_manager
        from medalflow.datalake.services import get_configuration_service

        # Get feature managers
        self._stats_manager: StatsProtocol | None = get_feature_manager("stats")
        self._cache_manager: CacheProtocol | None = get_feature_manager("cache")

        # Initialize configuration service to inject data loaders
        config_service = get_configuration_service()
        config_service.initialize()

        if self._stats_manager:
            self.logger.debug(
                "sequencer.stats_manager_initialized",
                extra=sanitize_extras({"sequencer": self.__class__.__name__}),
            )
        if self._cache_manager:
            self.logger.debug(
                "sequencer.cache_manager_initialized",
                extra=sanitize_extras({"sequencer": self.__class__.__name__}),
            )

    def get_layer_name(self) -> str:
        """Get the layer name for this sequencer.

        Subclasses should override this method to return their layer name.
        This is used for loading layer-specific configurations.

        Returns:
            str: Layer name ('bronze', 'silver', 'gold')
        """
        # Default implementation - subclasses should override
        return self.__class__.__name__.lower().replace("sequencer", "")

    def get_obj_name(self) -> str:
        """Get unique object name for this sequencer.

        Base implementation returns the class name. Subclasses should
        override this method to provide more specific naming based on
        their requirements.

        Returns:
            Object name for this sequencer (defaults to class name)
        """
        return self.__class__.__name__

    def get_stats_columns(self, table_name: str, layer: str | None = None) -> list[str] | None:
        """Get statistics columns for a table.

        Args:
            table_name: Name of the table
            layer: Layer name (uses get_layer_name() if not provided)

        Returns:
            List of column names that need statistics, or None
        """
        if not self._stats_manager:
            return None

        if layer is None:
            layer = self.get_layer_name()

        return self._stats_manager.get_stats_columns(table_name, layer)

    def clear_execution_plan_cache(self) -> None:
        """Clear cached execution plan for this sequencer.

        This method removes the cached execution plan from the cache manager,
        forcing the next call to get_execution_plan() to regenerate it.
        """
        if self._cache_manager:
            cache_key = f"{self.get_layer_name()}:execution_plan:{self.get_obj_name()}"
            self._cache_manager.delete(cache_key)
            self.logger.info(
                "sequencer.cache_cleared",
                extra=sanitize_extras(
                    {
                        "sequencer": self.get_obj_name(),
                        "cache_key": cache_key,
                    }
                ),
            )

    def get_queries(self) -> list[BaseOperation]:
        """Get all operations generated by this sequencer.

        This method returns BaseOperation instances that can be executed by compute engines.
        Uses the discovered methods, applies any layer-specific transformations, and
        automatically creates statistics operations when metadata.create_stats is True.

        Returns:
            List[BaseOperation]: List of operation instances in execution order

        Raises:
            ValueError: If the model's queries cannot be built. Authoring
                mistakes fail loudly at plan time rather than degrading to an
                empty plan.
        """
        try:
            discovered_methods = self._discover_methods()

            queries = self._get_queries(discovered_methods)

            return queries
        except Exception as e:
            model_name = self.get_obj_name()
            # DEBUG, not ERROR. This failure is *raised*, and `compile()`
            # collects it into a `CompileError` it returns -- writing a
            # traceback here too made a returned result look at a terminal
            # like a crash that happened to return a value, which is the
            # opposite of what "collected, not raised" promises. Nothing is
            # thrown away: the stack is still one log level down, and an
            # exception nobody catches is still loud, because it still
            # propagates.
            self.logger.debug(
                "sequencer.get_queries_failed",
                extra=sanitize_extras(
                    {
                        "error": str(e),
                        "sequencer": self.__class__.__name__,
                        "model": model_name,
                    }
                ),
                exc_info=True,
            )
            raise ValueError(
                f"Failed to build queries for model '{model_name}' "
                f"({self.__class__.__name__}): {e}"
            ) from e

    def _select(self, discovered_methods: list[DiscoveredMethod]) -> list[DiscoveredMethod]:
        """Narrow discovered methods to `self.selection`.

        This was gold's private override of `_get_queries`, but nothing about
        filtering a sequencer's methods by their target table is gold-specific.
        Held there, `selection` meant one thing in gold and nothing at all in
        silver, which is the "parameter that lied" shape ADR 002 D3 deleted.

        Args:
            discovered_methods: List of discovered methods with metadata and SQL

        Returns:
            Every method when `selection` is None, otherwise only those writing
            a table it names — an empty list included, which selects nothing.
        """
        if self.selection is None:
            return discovered_methods

        selected = [
            method for method in discovered_methods if method.metadata.table_name in self.selection
        ]

        if not selected:
            self.logger.warning(f"No methods found for selected tables: {self.selection}")

        return selected

    def _declared_schema(self) -> str:
        """The schema this model declares it writes into.

        Every layer decorator takes a `schema` (ADR 002 D2). Read off the
        class-level metadata, so bronze, silver and gold all answer the same
        way, and "" when the class carries no declaration at all.

        Returns:
            The declared schema, or "" when there is none
        """
        return self._get_class_metadata().get("schema") or ""

    def _inherit_schema(self, metadata: QueryMetadata) -> QueryMetadata:
        """Fill an omitted `schema_name` from the class's declared `schema`.

        `query_metadata(schema_name="")` means the method did not say, so it
        writes wherever its model writes. A method that *did* say keeps what it
        said: explicit beats inherited, or the class-level declaration would be
        a silent override.

        Applied here rather than in `_discover_methods` on purpose. Silver's
        `_transform_query_result` promotes a staged detail table by matching
        `metadata.schema_name` against `conventions.detail_tables.source_schema`
        -- and it runs during discovery, before this. Defaulting earlier would
        make that comparison see an inherited schema no author wrote, so a
        silver model whose declared `schema` happened to equal the configured
        source schema would have *every* suffix-matching method silently
        promoted. The convention keys on what the method declared; inheritance
        fills in afterwards, and the rewrite's own target schema is non-empty
        so it is left alone.

        Args:
            metadata: Query metadata from the decorator, possibly rewritten by
                `_transform_query_result`

        Returns:
            The metadata unchanged when it names a schema or the class declares
            none, otherwise a copy carrying the declared schema
        """
        if metadata.schema_name:
            return metadata

        declared = self._declared_schema()
        if not declared:
            return metadata

        inherited = metadata.model_copy()
        inherited.schema_name = declared

        return inherited

    def _get_queries(self, discovered_methods: list[DiscoveredMethod]) -> list[BaseOperation]:
        """Internal hook to extract operations from discovered methods.

        Default implementation applies `self.selection`, fills each method's
        omitted `schema_name` from the class's declared `schema`, then creates
        BaseOperation instances from each surviving method. Subclasses can
        override to apply transformations of their own.

        Args:
            discovered_methods: List of discovered methods with metadata and SQL

        Returns:
            List[BaseOperation]: List of operation instances
        """
        operations = []

        for method_name, _method, declared_metadata, sql in self._select(discovered_methods):
            metadata = self._inherit_schema(declared_metadata)

            if not sql:
                self.logger.info(
                    "sequencer.operation_missing_sql",
                    extra=sanitize_extras(
                        {
                            "method": method_name,
                            "sequencer": self.__class__.__name__,
                        }
                    ),
                )
                continue

            kwargs = {}
            if metadata.type == QueryType.CREATE_TABLE:
                kwargs["select_query"] = sql
            elif metadata.type == QueryType.CREATE_OR_ALTER_VIEW:
                kwargs["select_query"] = sql
            elif metadata.type in [QueryType.INSERT, QueryType.MERGE]:
                kwargs["source_query"] = sql
            elif metadata.type == QueryType.EXECUTE_SQL:
                kwargs["sql"] = sql

            try:
                operation = OperationBuilder.create_operation(
                    query_type=metadata.type,
                    schema_name=metadata.schema_name,
                    object_name=metadata.table_name,
                    engine_hint=metadata.preferred_engine,
                    logging_context={
                        "method": method_name,
                        "class": self.__class__.__name__,
                        "source": self._get_method_source(method_name),
                        "name": self.get_obj_name(),
                        "layer": self.get_layer_name(),
                    },
                    metadata=metadata,
                    **kwargs,
                )
                operations.append(operation)
            except Exception as e:
                # DEBUG for the reason above: re-raised on the line below, so
                # whoever handles it decides how loudly it is reported.
                self.logger.debug(
                    "sequencer.operation_create_failed",
                    extra=sanitize_extras(
                        {
                            "method": method_name,
                            "sequencer": self.__class__.__name__,
                            "error": str(e),
                        }
                    ),
                    exc_info=True,
                )
                raise

        return operations
