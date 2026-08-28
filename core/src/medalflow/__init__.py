
from medalflow.__version__ import __version__
from medalflow.api import (
    execute,
    get_bronze_execution_plan,
    get_execution_plan_for_sps,
    get_gold_execution_plan,
    get_silver_execution_plan_for_models,
)
from medalflow.medallion import (
    GoldSequencer,
    SilverTransformationSequencer,
    SnapshotSequencer,
    gold_metadata,
    query_metadata,
    silver_metadata,
    snapshot_metadata,
)

# Backward compatibility aliases
etl_metadata = silver_metadata  # Backward compatibility alias
view_metadata = gold_metadata   # Backward compatibility alias
SilverSequencer = SilverTransformationSequencer  # Backward compatibility alias


from medalflow.common.exceptions import CTEError, ErrorCode

# Utils (public API)
from medalflow.utils import (
    # DateTime utilities
    get_current_timestamp,
    get_partition_path,
    get_snapshot_datetime,
    parse_snapshot_path,
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

    #api
    "get_bronze_execution_plan",
    "get_gold_execution_plan",
    "get_execution_plan_for_sps",
    "get_silver_execution_plan_for_models",
    "execute"
]