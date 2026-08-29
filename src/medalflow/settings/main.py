"""The MedalFlow settings object.

One :class:`MedalflowSettings` reads every configuration value MedalFlow needs.
Identity (``source_system``, ``ds_env``, ``name``) lives once, at the top level;
domain groups are plain pydantic models reached through the nested delimiter.

All environment variables share the ``MEDALFLOW_`` prefix and use ``__`` to
descend into a group::

    MEDALFLOW_NAME=fin
    MEDALFLOW_COMPUTE__LAKE_DATABASE_NAME=lakedb
    MEDALFLOW_DATALAKE__PROCESSED__ACCOUNT_NAME=mylake

See ``.env.example`` at the repository root for the full minimal set.
"""

import logging
from typing import TYPE_CHECKING, Any, Optional

from pydantic import (
    AliasChoices,
    BaseModel,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from medalflow.core.mixins import NestedSecretsMixin
from medalflow.secret_vault.env import ENV_PREFIX, EnvSecretProvider
from medalflow.secret_vault.keyvault import KeyVaultSecrets
from medalflow.secret_vault.mock import MockSecrets

from .compute import ComputeSettings
from .conventions import ConventionsSettings
from .datalake import MultiDataLakeSettings
from .features import FeatureSettings
from .keyvault import KeyVaultSettings
from .stats import StatsSettings

if TYPE_CHECKING:
    from medalflow.protocols.providers import SecretProvider

# app_env values that mean "real data, real secrets".
PRODUCTION_ENVIRONMENTS = frozenset({"prod", "production"})

# Layers whose models are discovered by walking a configured Python package.
MODEL_LAYERS = ("bronze", "silver", "gold")


class MedalflowSettings(NestedSecretsMixin, BaseSettings):
    """Every configuration value MedalFlow reads, in one object."""

    model_config = SettingsConfigDict(
        env_prefix="MEDALFLOW_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        env_nested_delimiter="__",
    )

    # --- Identity: declared once, read by every layer -----------------------
    source_system: str = Field(
        ..., description="Source system name (e.g., sap, oracle, dynamics365, salesforce)"
    )
    ds_env: str = Field(
        ...,
        description="Data source environment (dev, qa, uat, prod). Ensures environment isolation in the data lake.",
    )
    name: str = Field(
        ...,
        description="Short data source name. The single source of both the table prefix "
        "('fin_') and the external data source names ('ds_fin_proc'). "
        "Use short, descriptive names (e.g., sap, oracle, d365, sf).",
    )

    app_env: str = Field(
        default="dev",
        description="Application deployment environment (e.g., dev, qa, uat, prod, local)",
    )

    test_mode: bool = Field(
        default=False,
        validation_alias=AliasChoices("MEDALFLOW_TEST_MODE", "CTE_TEST_MODE"),
        description="Run with mock secrets instead of Azure Key Vault. Refused when "
        "app_env names a production environment. "
        "CTE_TEST_MODE is a deprecated alias for MEDALFLOW_TEST_MODE.",
    )

    max_retries: int = Field(
        default=3, ge=0, le=10, description="Maximum number of retry attempts for operations"
    )
    retry_delay_seconds: float = Field(
        default=1.0, ge=0.1, le=60.0, description="Delay between retry attempts in seconds"
    )
    max_workers: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Maximum number of worker threads for concurrent operations",
    )

    models_package: str | None = Field(
        default=None,
        description="Python package holding this project's models. Each layer is "
        "its subpackage: models_package='acme' means 'acme.bronze', 'acme.silver', "
        "'acme.gold'. Optional -- a layer can instead be named directly by its own "
        "override below. Discovery, not boot, is where an unconfigured layer fails.",
    )
    bronze_package: str | None = Field(
        default=None,
        description="Python package holding the bronze models. Overrides "
        "'{models_package}.bronze'.",
    )
    silver_package: str | None = Field(
        default=None,
        description="Python package holding the silver models. Overrides "
        "'{models_package}.silver'.",
    )
    gold_package: str | None = Field(
        default=None,
        description="Python package holding the gold models. Overrides " "'{models_package}.gold'.",
    )

    bronze_introspection: bool = Field(
        default=False,
        description="Derive one bronze model per table from a live "
        "INFORMATION_SCHEMA query instead of from declared @bronze_metadata "
        "models. Off by default, because turning it on costs offline compile: "
        "compiling then requires a reachable warehouse, for the bronze layer "
        "only -- silver and gold still answer from their packages. Everything "
        "downstream is unchanged, so a selector narrows introspected tables "
        "exactly as it narrows declared ones. The mode is never inferred from "
        "whether models are found, so a mistyped bronze_package fails loudly "
        "rather than falling back to the warehouse.",
    )

    configured_models: str = Field(
        default="",
        description=(
            "Comma-separated list of silver model groups to discover, matched "
            "against each silver transformation's `model=` "
            "(e.g. 'sales,purchase,inventory,finance'). Narrowing only: unset "
            "-- the default -- discovers every silver model rather than none. "
            "Bronze and gold declare no `model=` and are never filtered by it."
        ),
    )

    # --- Domain groups ------------------------------------------------------
    datalake: MultiDataLakeSettings = Field(
        default_factory=MultiDataLakeSettings, description="ADLS configuration"
    )
    compute: ComputeSettings = Field(
        ..., description="Compute platform configuration (SQL engines)"
    )
    keyvault: KeyVaultSettings = Field(
        default_factory=KeyVaultSettings, description="Key Vault configuration"
    )
    features: FeatureSettings = Field(
        default_factory=FeatureSettings, description="Feature flags configuration"
    )
    stats: StatsSettings = Field(
        default_factory=StatsSettings, description="Statistics management configuration"
    )
    conventions: ConventionsSettings = Field(
        default_factory=ConventionsSettings,
        description="Opt-in naming conventions; every one of them defaults to off",
    )

    @model_validator(mode="after")
    def refuse_test_mode_in_production(self) -> "MedalflowSettings":
        """Stop mock secrets from ever standing in for real ones in production.

        Returns:
            Self, unchanged, when the combination is allowed

        Raises:
            ValueError: If test mode is requested while app_env names production
        """
        if self.test_mode and self.app_env.strip().lower() in PRODUCTION_ENVIRONMENTS:
            raise ValueError(
                f"test_mode substitutes mock secrets for real ones and cannot run with "
                f"app_env={self.app_env!r}. Unset MEDALFLOW_TEST_MODE (or the deprecated "
                f"CTE_TEST_MODE alias), or point MEDALFLOW_APP_ENV at a non-production "
                f"environment."
            )

        return self

    @field_validator("configured_models")
    @classmethod
    def validate_configured_models(cls, v: str) -> str:
        """Validate and normalize the comma-separated model list.

        Args:
            v: The configured_models string value

        Returns:
            Validated and normalized model string
        """
        if not v:
            return v

        models = [m.strip() for m in v.split(",") if m.strip()]

        for model in models:
            if not model.replace("_", "").replace("-", "").isalnum():
                raise ValueError(
                    f"Invalid model name '{model}'. "
                    f"Model names must be alphanumeric with optional underscores or hyphens."
                )

        return ",".join(models)

    def model_post_init(self, __context) -> None:
        """Wire the object graph once pydantic has validated every field.

        1. Hands the top-level ``name`` to the compute group, so the external
           data source names derive from the same field as ``table_prefix``.
        2. Creates the secret provider (Key Vault, or mock in test mode) and
           propagates it to the groups that carry ``SecretField`` descriptors.

        Args:
            __context: Pydantic context (internal use)
        """
        super().model_post_init(__context)

        self.compute.bind_data_source_name(self.name)

        secret_provider = self._create_secret_provider()
        if secret_provider:
            self.attach_secrets(secret_provider)
            self.propagate_secrets(self.compute, self.datalake)

    def _create_secret_provider(self) -> "SecretProvider":
        """Create the secret provider named by the configuration.

        Never returns None: a settings object without a provider is a settings
        object whose ``SecretField`` descriptors all read empty.

        Returns:
            A KeyVault provider if Key Vault is configured, a mock provider in
            test mode, otherwise the zero-dependency environment provider.
        """
        logger = logging.getLogger(__name__)

        if self.keyvault.is_configured:
            try:
                logger.debug("Creating KeyVault secret provider")
                return KeyVaultSecrets(settings=self.keyvault)
            except Exception as e:
                # Was `return None`, the last route to a provider-less settings
                # object. The environment provider needs nothing installed, so
                # falling back to it beats having no provider at all.
                logger.warning(
                    "Failed to create KeyVault provider (%s); falling back to %s* variables",
                    e,
                    ENV_PREFIX,
                )
                return EnvSecretProvider()

        elif self.test_mode:
            logger.info("Using mock secret provider for test mode")
            return MockSecrets()

        logger.info("Key Vault is not configured; reading secrets from %s* variables", ENV_PREFIX)
        return EnvSecretProvider()

    @property
    def secrets(self) -> Optional["SecretProvider"]:
        """The attached secret provider, created on first access if needed."""
        if self._secret_provider is None:
            self._secret_provider = self._create_secret_provider()
        return self._secret_provider

    # --- Derived paths and names -------------------------------------------
    @property
    def base_path(self) -> str:
        """Base path for this data source inside the file system."""
        return self.ds_env

    @property
    def datasource_file_system(self) -> str:
        """File system (container) name for this data source."""
        return self.source_system.lower()

    @property
    def full_path(self) -> str:
        """Full prefix including file system, for display purposes."""
        return f"{self.datasource_file_system}/{self.base_path}"

    @property
    def table_prefix(self) -> str:
        """Prefix applied to table names, derived from ``name``."""
        return f"{self.name}_"

    @property
    def ds_name(self) -> str:
        """Data source name used for package naming."""
        return self.name

    def get_configured_model_list(self) -> list[str]:
        """The silver model groups this deployment narrowed discovery to.

        Returns:
            The configured model names. Empty means no list was configured,
            which ``is_model_configured`` reads as "no filter" -- not as
            "no models".
        """
        if not self.configured_models:
            return []
        return [m.strip() for m in self.configured_models.split(",")]

    def is_model_configured(self, model_name: str) -> bool:
        """Whether silver discovery should keep a model with this ``model=``.

        An unset ``configured_models`` filters nothing. This asked whether the
        name was *in* the list, and the list is empty until someone populates
        it -- so a project that had never heard of the setting had every silver
        model skipped and its layer compiled to nothing, with no error saying
        why. A filter nobody configured must not delete the thing it filters,
        which is also what ``selection=None`` means everywhere else here:
        absent is everything, empty is nothing.

        Args:
            model_name: The ``model=`` a silver transformation declares

        Returns:
            True when no list is configured, otherwise whether the list names
            this model
        """
        configured = self.get_configured_model_list()

        if not configured:
            return True

        return model_name in configured

    # --- Where the models live ----------------------------------------------
    def package_for_layer(self, layer: str) -> str:
        """Python package to walk for one layer's models.

        Resolution order: the layer's own override, then ``models_package``
        with the layer appended.

        Args:
            layer: One of 'bronze', 'silver', 'gold'

        Returns:
            Importable package path for that layer

        Raises:
            ValueError: If the layer is unknown, or if neither the layer
                override nor ``models_package`` is configured
        """
        if layer not in MODEL_LAYERS:
            raise ValueError(f"Unknown layer {layer!r}. Expected one of {', '.join(MODEL_LAYERS)}.")

        override = getattr(self, f"{layer}_package")
        if override:
            return override

        if self.models_package:
            return f"{self.models_package}.{layer}"

        raise ValueError(
            f"No Python package is configured for the {layer} layer, so there is "
            f"nothing to discover models in. Set MEDALFLOW_MODELS_PACKAGE to the "
            f"package holding your models (its '{layer}' subpackage is walked), or "
            f"set MEDALFLOW_{layer.upper()}_PACKAGE to name the {layer} package "
            f"directly."
        )


class SettingsError(ValueError):
    """MedalFlow could not construct its settings.

    A :class:`ValueError`, like the ``ValidationError`` it replaces, so no
    existing ``except`` clause has to learn a new name.
    """


def _env_var(loc: tuple[str, ...]) -> str:
    """The environment variable one settings field is read from.

    Derived from the settings object's own ``env_prefix`` and
    ``env_nested_delimiter`` rather than looked up, so it cannot fall out of
    step with the configuration it describes.

    Args:
        loc: The field's location, as pydantic reports it -- ``('name',)``, or
            ``('compute', 'lake_database_name')`` for a field inside a group

    Returns:
        The variable name, e.g. ``MEDALFLOW_COMPUTE__LAKE_DATABASE_NAME``
    """
    config = MedalflowSettings.model_config
    delimiter = config.get("env_nested_delimiter") or ""

    return f"{config.get('env_prefix', '')}{delimiter.join(loc)}".upper()


def _required_fields_of(model: type[BaseModel], loc: tuple[str, ...]) -> list[tuple[str, ...]]:
    """Every required field under a settings group, located from its parent.

    Pydantic reports a missing group as the group -- ``compute`` -- because
    that is the field it could not fill. What the user has to set is inside
    it, so the group is expanded into the fields that actually have no value,
    descending through nested groups the same way.

    Args:
        model: The group's model class
        loc: Where the group itself sits, e.g. ``('compute',)``

    Returns:
        One location per required field. The group's own location when it has
        no required field, which cannot happen for a group pydantic reported
        as missing but keeps the return honest.
    """
    required: list[tuple[str, ...]] = []

    for field_name, field in model.model_fields.items():
        if not field.is_required():
            continue

        annotation = field.annotation
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            required.extend(_required_fields_of(annotation, loc + (field_name,)))
        else:
            required.append(loc + (field_name,))

    return required or [loc]


def _locations_of(loc: tuple[str, ...]) -> list[tuple[str, ...]]:
    """Expand one reported location into the fields a user can actually set.

    Args:
        loc: A location from ``ValidationError.errors()``

    Returns:
        The location itself, or -- when it names a group rather than a value
        -- every required field inside that group
    """
    annotation: Any = MedalflowSettings

    for part in loc:
        fields = getattr(annotation, "model_fields", None)
        if not fields or part not in fields:
            return [loc]
        annotation = fields[part].annotation

    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return _required_fields_of(annotation, loc)

    return [loc]


def _describe(loc: tuple[str, ...], fallback: str) -> str:
    """What to say about one variable: its field's own description.

    Args:
        loc: The field's location
        fallback: Pydantic's message, used when the field carries no
            description of its own

    Returns:
        A single line of guidance
    """
    annotation: Any = MedalflowSettings
    description = None

    for part in loc:
        fields = getattr(annotation, "model_fields", None)
        if not fields or part not in fields:
            return fallback
        description = fields[part].description
        annotation = fields[part].annotation

    return description or fallback


def _boot_error(error: ValidationError) -> SettingsError:
    """Rewrite a settings ``ValidationError`` in environment-variable terms.

    Pydantic reports the fields it could not fill: ``source_system``,
    ``ds_env``, ``name``, ``compute``. Those are not what a user sets, and
    ``compute`` -- a group whose one required field is
    ``MEDALFLOW_COMPUTE__LAKE_DATABASE_NAME`` -- is actively unhelpful.

    Only a *missing* field is expanded into the variables to set. A field that
    was given a value pydantic refused, and a whole-object rule like
    `refuse_test_mode_in_production`, already say something worth reading --
    rewriting those as "set these variables" would replace a real explanation
    with a list, which is the failure mode this function exists to fix.

    Args:
        error: What constructing :class:`MedalflowSettings` raised

    Returns:
        The same failure, naming the variables to set
    """
    lines: list[str] = []
    seen: set[str] = set()
    rejected: list[str] = []

    for detail in error.errors():
        loc = tuple(str(part) for part in detail["loc"])

        if not loc:
            # A rule about the object as a whole names no field, so there is
            # no variable to derive. It carries its own message.
            rejected.append(detail["msg"])
            continue

        missing = detail["type"] == "missing"
        for location in _locations_of(loc) if missing else [loc]:
            variable = _env_var(location)
            if variable in seen:
                continue

            seen.add(variable)
            said = _describe(location, detail["msg"]) if missing else detail["msg"]
            lines.append(f"  {variable}: {said}")

    parts = ["MedalFlow could not read its configuration."]

    if lines:
        parts.append("Set these environment variables:\n\n" + "\n".join(lines))
    if rejected:
        parts.extend(rejected)

    parts.append(
        "They are read from the process environment and from a '.env' file in "
        "the working directory. See '.env.example'."
    )

    return SettingsError("\n\n".join(parts))


# Singleton instance
_settings: MedalflowSettings | None = None


def get_settings(force_reload: bool = False) -> MedalflowSettings:
    """Get the singleton settings instance for the application.

    Settings are loaded from environment variables (and ``.env``) on first
    access and reused, so configuration stays consistent across components and
    Key Vault is contacted at most once per process.

    Args:
        force_reload: If True, build a new instance even if one already exists.
                      Useful for testing or when the environment has changed.

    Returns:
        The singleton MedalflowSettings instance

    Raises:
        SettingsError: If the configuration is incomplete. The message names
            the environment variables to set, not the pydantic fields that
            could not be filled.

    Example:
        >>> settings = get_settings()
        >>> get_settings() is settings
        True
        >>> get_settings(force_reload=True) is settings
        False

    Note:
        Reading is thread-safe; the initial creation is not. Settings are
        normally loaded once at startup, before threading begins.
    """
    global _settings

    if _settings is None or force_reload:
        try:
            _settings = MedalflowSettings()
        except ValidationError as e:
            # Pydantic names fields; a user sets variables. Translating is the
            # difference between 'compute -- Field required' and
            # 'MEDALFLOW_COMPUTE__LAKE_DATABASE_NAME'.
            raise _boot_error(e) from e

    return _settings
