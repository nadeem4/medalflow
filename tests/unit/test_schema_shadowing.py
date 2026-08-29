"""`schema` is a field name in every layer's metadata, and pydantic warns.

Pydantic emits a `UserWarning` when a field shadows an attribute of
`BaseModel` -- here the deprecated v1 `.schema()` shim. Shadowing it is the
point: ADR 002 D2 makes `schema` the layers' word for their write target, and
nothing calls the shim.

The suppression was copy-pasted per class, because the warning text names the
class. Three copies is two too many, so it is one helper -- and these tests pin
what that helper must do: silence *that* warning, and nothing else.
"""

import importlib.util
import warnings

import pytest
from medalflow.types.metadata import (
    BronzeMetadata,
    GoldMetadata,
    SilverMetadata,
    _shadowing_schema_is_intended,
)

LAYER_MODELS = [BronzeMetadata, SilverMetadata, GoldMetadata]


def _load_fresh():
    """Load the module again under a throwaway name.

    A `reload` would rebind the real module's classes, and the `ClassMetadata`
    union and every `isinstance` check downstream depend on their identity.
    """
    import medalflow.types.metadata as real

    spec = importlib.util.spec_from_file_location("_metadata_under_test", real.__file__)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_defining_the_layer_models_raises_no_user_warning():
    """The CI gate is `python -W error::UserWarning -c "import medalflow"`."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)

        module = _load_fresh()

    assert module.SilverMetadata.model_fields["schema"] is not None


@pytest.mark.parametrize("model", LAYER_MODELS, ids=lambda m: m.__name__)
def test_schema_reaches_the_declared_value_not_the_deprecated_shim(model):
    """Suppressing the warning is only worth it if the field actually wins."""
    field_names = set(model.model_fields)
    values = {name: "x" for name in field_names if model.model_fields[name].is_required()}

    assert model(**{**values, "schema": "warehouse"}).schema == "warehouse"


def test_the_helper_lets_every_other_user_warning_through():
    """A blanket `simplefilter("ignore")` would swallow real problems."""
    with pytest.raises(UserWarning, match="something else"):
        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            with _shadowing_schema_is_intended():
                warnings.warn("something else entirely", UserWarning, stacklevel=1)


def test_the_helper_silences_the_schema_shadow_warning():
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)

        with _shadowing_schema_is_intended():
            warnings.warn(
                'Field name "schema" in "Anything" shadows an attribute in parent "BaseModel"',
                UserWarning,
                stacklevel=1,
            )
