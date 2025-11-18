
from core.__version__ import __version__
from core.medallion import (
    SilverTransformationSequencer,
    GoldSequencer,
    SnapshotSequencer,
    silver_metadata,
    gold_metadata,
    snapshot_metadata,
    query_metadata,
)

from core.api import (
    get_bronze_execution_plan,
    get_gold_execution_plan,
    get_execution_plan_for_sps,
    get_silver_execution_plan_for_models,
    execute
)

# Backward compatibility aliases
etl_metadata = silver_metadata  # Backward compatibility alias
view_metadata = gold_metadata   # Backward compatibility alias
SilverSequencer = SilverTransformationSequencer  # Backward compatibility alias


from core.common.exceptions import CTEError, ErrorCode

# Utils (public API)
from core.utils import (
    # DateTime utilities
    get_current_timestamp,
    get_snapshot_datetime,
    get_partition_path,
    parse_snapshot_path,
    # Decorators
    retry,
    async_retry,
)



__all__ = [
    "__version__",
    
    "SilverTransformationSequencer",
    "SilverSequencer",
    "GoldSequencer",
    "SnapshotSequencer",
    
    "silver_metadata",
    "gold_metadata",
    "snapshot_metadata",
    "query_metadata",
    "etl_metadata",  # Backward compatibility alias
    "view_metadata",  # Backward compatibility alias
    
    # Exceptions (public API)
    "CTEError",
    "ErrorCode",
    
    # Utilities (public API)
    "get_current_timestamp",
    "get_snapshot_datetime",
    "get_partition_path",
    "parse_snapshot_path",
    "retry",
    "async_retry",

    #api
    "get_bronze_execution_plan",
    "get_gold_execution_plan",
    "get_execution_plan_for_sps",
    "get_silver_execution_plan_for_models",
    "execute"
]