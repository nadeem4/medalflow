"""Statistics configuration settings.

This module provides configuration settings for database statistics
management across compute platforms. The actual statistics logic is
handled by the StatsManager feature plugin.
"""

from pydantic import BaseModel, Field
from typing import List

from pydantic_settings import SettingsConfigDict


class StatsSettings(BaseModel):
    """Statistics configuration settings.
    
    This class contains only configuration data for statistics management.
    The actual logic for determining when and how to create statistics
    is handled by the StatsManager feature plugin in core.features.
    
    Configuration includes:
    - Path to CSV file containing detailed stats configuration
    - Lists of tables that should have statistics created
    """

    stats_csv_path: str = Field(
        default="client_configuration/external_table_stats.csv",
        description="Path to stats configuration CSV in Internal ADLS"
    )

    bronze_tables_for_stats: List[str] = Field(
        default=[
            "InventTrans",
            "InventSum",
            "GeneralJournalAccountEntry",
            "GeneralJournalEntry",
            "InventDim",
            "InventJournalTrans",
            "InventJournalTable",
            "InventTransPosting",
            "SalesLine",
            "SalesTable",
        ],
        description="List of bronze tables that should have statistics created (defaults are for D365)",
    )
    
    silver_tables_for_stats: List[str] = Field(
        default=[
            "Product",
            "ProductReceiptLineTrans_Fact",
            "ProductReceiptLine_Fact",
            "Vendor",
            "Tag",
            "InventoryOnHand_Fact",
            "TagAttribute",
            "FiscalDate",
            "AgingBucket",
            "SalesOrderLineCharge_Fact",
            "SalesOrderLineTrans_Fact",
            "SalesInvoiceLineTrans_Fact",
        ],
        description="List of silver tables that should have statistics created (defaults are for D365)", 
    )
