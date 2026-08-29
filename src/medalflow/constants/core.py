from enum import Enum


class LayerType(str, Enum):
    """Layer structure type for package naming.

    Read by nothing. Model packages are configuration now
    (`MEDALFLOW_MODELS_PACKAGE` and the per-layer overrides), so neither the
    conventions below nor the `layer_type` setting that selects between them
    influences any import path. Retained pending removal.

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
