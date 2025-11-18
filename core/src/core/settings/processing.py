from typing import List, Dict, Any
import pytz
from pydantic import Field, field_validator, model_validator

from .base import CTEBaseSettings
from core.constants import CalendarType


class ProcessingSettings(CTEBaseSettings):
    
   
    time_zone: str = Field(
        default="US/Central",
        description="The time zone used for date/time calculations, e.g., source cut-off times"
    )
    
    is_445_calendar: bool = Field(
        default=False,
        description="Flag for using a 4-4-5 retail calendar instead of standard calendar"
    )
    
    balance_refresh_period_in_months: int = Field(
        default=12,
        ge=1,
        le=60,
        description="The look-back period in months for refreshing balance data"
    )
    
    balance_storage_period_in_months: int = Field(
        default=24,
        ge=1,
        le=120,
        description="The total period in months to store balance data"
    )
    
    future_months: int = Field(
        default=18,
        ge=0,
        le=60,
        description="The number of future months to project or include in financial calculations"
    )

    is_inverse_conversion: bool = Field(
        default=False,
        description="Flag for inverse unit of measure conversion"
    )
    
    uom_version: str = Field(
        default="V1",
        description="The version of the Unit of Measure conversion tables to use"
    )
    
    # Integration settings
    dataverse_shift_prefix: str = Field(
        default="cmaa_",
        description="The prefix to be used for Dataverse table shifts"
    )
    
    
    @field_validator('time_zone')
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        """Validate that the timezone is valid."""
        try:
            pytz.timezone(v)
            return v
        except pytz.exceptions.UnknownTimeZoneError:
            raise ValueError(f"Unknown timezone: {v}. Use pytz timezone names like 'US/Central'")

    
    @field_validator('uom_version')
    @classmethod
    def validate_uom_version(cls, v: str) -> str:
        """Validate UoM version format."""
        if not v or not v.strip():
            raise ValueError("UoM version cannot be empty")
        if not v.startswith('V'):
            raise ValueError("UoM version must start with 'V' (e.g., 'V1', 'V2')")
        return v.upper()
    
    @model_validator(mode='after')
    def validate_balance_periods(self) -> 'ProcessingSettings':
        """Validate balance period relationships."""
        # Check that storage period is not less than refresh period
        if self.balance_storage_period_in_months < self.balance_refresh_period_in_months:
            raise ValueError(
                f"Balance storage period ({self.balance_storage_period_in_months} months) "
                f"must be >= refresh period ({self.balance_refresh_period_in_months} months)"
            )
        
        return self
    
    @property
    def calendar_type(self) -> CalendarType:
        """Get the calendar type as a string.
        
        Returns:
            '4-4-5' or 'standard'
        """
        return CalendarType.FOUR_FOUR_FIVE if self.is_445_calendar else CalendarType.STANDARD
    
    @property
    def timezone_info(self) -> pytz.tzinfo:
        """Get the pytz timezone object.
        
        Returns:
            pytz timezone object for the configured timezone
        """
        return pytz.timezone(self.time_zone)
    
    
