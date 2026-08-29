"""Selector grammar v0.1 (ADR 002, Decision 7).

The grammar is tiny on purpose: `*`, `layer:<layer>`, `tag:<value>` and a bare
model name. Two things it does beyond matching, and both are the point:

* it *recognises* `+name` and `name+` -- the v0.3 graph operators -- and
  rejects them by name, so adding them later changes behaviour and not the
  grammar or any call site;
* an unparseable selector raises. A typo must not read as "nothing matched".

Selectors match on `name`, `layer` and `tags`. Never on `_dag_id`: it carries a
positional `_{i}` suffix, so it is not stable across selections.
"""

import pytest
from medalflow.api.selectors import SelectorError, parse_selector


class _Model:
    """The three fields a selector is allowed to read."""

    def __init__(self, name, layer, tags=()):
        self.name = name
        self.layer = layer
        self.tags = list(tags)


CUSTOMERS = _Model("Customers", "bronze", ["domain:sales"])
DIM_CUSTOMER = _Model("DimCustomer", "silver", ["daily"])
REVENUE = _Model("Revenue", "gold", ["daily", "domain:sales"])

EVERY_MODEL = [CUSTOMERS, DIM_CUSTOMER, REVENUE]


def _selected(selector):
    return [model.name for model in EVERY_MODEL if parse_selector(selector).matches(model)]


# --- the grammar -----------------------------------------------------------


def test_star_selects_everything():
    assert _selected("*") == ["Customers", "DimCustomer", "Revenue"]


def test_layer_selects_one_layer():
    assert _selected("layer:bronze") == ["Customers"]
    assert _selected("layer:silver") == ["DimCustomer"]
    assert _selected("layer:gold") == ["Revenue"]


def test_tag_selects_every_model_carrying_it():
    assert _selected("tag:daily") == ["DimCustomer", "Revenue"]


def test_a_bare_word_selects_by_model_name():
    assert _selected("Revenue") == ["Revenue"]


def test_a_bare_word_matching_nothing_is_an_empty_selection_not_an_error():
    """Narrowing to a name a project does not declare is an empty plan."""
    assert _selected("no_such_model") == []


def test_whitespace_around_a_selector_is_ignored():
    assert _selected("  layer:gold  ") == ["Revenue"]


# --- the v0.3 tokens, recognised and refused -------------------------------


@pytest.mark.parametrize("selector", ["+Revenue", "Revenue+", "+Revenue+"])
def test_graph_operators_are_recognised_and_refused_by_name(selector):
    with pytest.raises(SelectorError, match="not yet supported"):
        parse_selector(selector)


# --- unparseable selectors raise -------------------------------------------


def test_an_unknown_prefix_is_an_error_not_an_empty_selection():
    with pytest.raises(SelectorError, match="owner"):
        parse_selector("owner:nadeem")


def test_an_unknown_layer_is_an_error():
    with pytest.raises(SelectorError, match="platinum"):
        parse_selector("layer:platinum")


def test_an_empty_selector_is_an_error():
    with pytest.raises(SelectorError):
        parse_selector("   ")


def test_a_prefix_with_no_value_is_an_error():
    with pytest.raises(SelectorError):
        parse_selector("tag:")


def test_selector_error_is_a_value_error():
    """Callers catching ValueError around the public API keep working."""
    assert issubclass(SelectorError, ValueError)
