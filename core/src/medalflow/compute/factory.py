"""Platform creation.

Builds the compute platform named by ``settings.compute.compute_type``.
"""

from typing import TYPE_CHECKING

from medalflow.compute.platforms.base import _BasePlatform
from medalflow.compute.platforms.synapse import SynapsePlatform
from medalflow.constants import ComputeType
from medalflow.constants.compute import ComputeEnvironment
from medalflow.logging import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


def create_platform(
    environment: ComputeEnvironment = ComputeEnvironment.ETL,
) -> _BasePlatform:
    """Create the configured compute platform.

    Args:
        environment: Compute environment (ETL or CONSUMPTION).

    Returns:
        A platform instance configured from settings.

    Raises:
        ValueError: If ``settings.compute.compute_type`` names no known platform.

    Example:
        >>> platform = create_platform()
        >>> platform = create_platform(ComputeEnvironment.CONSUMPTION)
    """
    from medalflow.settings import get_settings

    settings = get_settings()
    platform_type = settings.compute.compute_type

    if platform_type == ComputeType.SYNAPSE:
        platform = SynapsePlatform(settings.compute.synapse, environment)
    else:
        raise ValueError(
            f"Unsupported compute type: {platform_type}. Supported types: SYNAPSE"
        )

    logger.info(
        "Created platform",
        extra={
            "platform": platform_type.value,
            "environment": environment.value,
        },
    )
    return platform
