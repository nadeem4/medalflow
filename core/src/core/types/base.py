"""Base model class for all medalflow models with serialization support."""

from typing import Any, Dict
from pydantic import BaseModel, ConfigDict


class CTEBaseModel(BaseModel):
    """Base model for all medalflow models with built-in serialization.
    
    Provides common functionality for all medalflow models including:
    - Serialization to dictionary via to_dict()
    - Consistent configuration
    - Proper handling of nested models
    """
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        use_enum_values=True,  
        validate_assignment=True
    )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary for serialization.
        
        Recursively converts nested CTEBaseModel instances to dictionaries.
        Uses Pydantic's model_dump with proper handling of nested models.
        
        Returns:
            Dictionary representation suitable for JSON serialization
        """
        # Don't use mode parameter initially to preserve nested model instances
        data = self.model_dump(by_alias=False, exclude_none=True)
        
        # Recursively handle nested CTEBaseModel instances and enums
        def convert_nested(obj):
            if isinstance(obj, CTEBaseModel):
                # Recursively call to_dict on nested models
                return obj.to_dict()
            elif isinstance(obj, dict):
                return {k: convert_nested(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_nested(item) for item in obj]
            elif hasattr(obj, 'value'):  # Handle enums
                return obj.value
            return obj
            
        return convert_nested(data)
        
        