from enum import Enum


class LayerType(str, Enum):
    """Layer structure type for package naming.
    
    Controls how Python packages are structured for business logic.
    This affects import paths for silver, gold, and snapshot transformations.
    
    Values:
        BASE: Traditional package structure
            - Format: {name}.layers.custom.{layer}
            - Example: "fin.layers.custom.silver"
            - Used for standard deployments
            
        CUSTOM: Simplified package structure
            - Format: custom_{name}.{layer}
            - Example: "custom_fin.silver"
            - Used for custom client deployments
    """
    BASE = "base"
    CUSTOM = "custom"