"""silver.Alpha reads silver.Beta, and silver.Beta reads silver.Alpha."""

from medalflow.constants.sql import QueryType
from medalflow.medallion.base.decorators import query_metadata
from medalflow.medallion.silver import SilverTransformationSequencer, silver_metadata


@silver_metadata(name="usp_load_alpha", schema="silver", model="sales")
class Alpha(SilverTransformationSequencer):
    """Reads the model that reads it."""

    @query_metadata(type=QueryType.CREATE_TABLE, table_name="Alpha")
    def build_alpha(self) -> str:
        return "SELECT Id FROM silver.Beta"


@silver_metadata(name="usp_load_beta", schema="silver", model="sales")
class Beta(SilverTransformationSequencer):
    """Reads the model that reads it."""

    @query_metadata(type=QueryType.CREATE_TABLE, table_name="Beta")
    def build_beta(self) -> str:
        return "SELECT Id FROM silver.Alpha"
