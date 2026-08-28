"""Query builder creation.

Builds the query builder for the platform named by
``settings.compute.compute_type``.
"""

from medalflow.constants.compute import ComputeType
from medalflow.query_builder.synapse.serverless_builder import (
    SynapseServerlessQueryBuilder,
)


def create_query_builder() -> SynapseServerlessQueryBuilder:
    """Create the query builder for the configured compute platform.

    Returns:
        A query builder configured from settings. Synapse is the only
        platform currently implemented, so the concrete return type is
        ``SynapseServerlessQueryBuilder``.

    Raises:
        ValueError: If ``settings.compute.compute_type`` names no known platform.

    Example:
        >>> builder = create_query_builder()
        >>> sql = builder.build_query(operation)
    """
    from medalflow.settings import get_settings

    settings = get_settings()
    active_type = settings.compute.compute_type

    if active_type != ComputeType.SYNAPSE:
        raise ValueError(
            f"Unsupported compute type: {active_type}. Supported types: SYNAPSE"
        )

    return SynapseServerlessQueryBuilder(settings)
