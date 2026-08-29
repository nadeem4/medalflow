"""Selector grammar v0.1 (ADR 002, Decision 7).

A selector names the subset of a project's models a command applies to. The
whole grammar is four forms::

    *                everything
    layer:bronze     one medallion layer -- bronze, silver or gold
    tag:daily        every model carrying that tag
    Revenue          one model, by the `name` its decorator declares

Selectors match on ``name``, ``layer`` and ``tags``, and on nothing else. In
particular never on ``_dag_id``: it carries a positional ``_{i}`` suffix, so
the same model has a different id under a different selection.

A ``layer:`` selector is also the one form that can be answered before
anything is discovered, so it is the one form that prunes discovery rather than
only filtering its results -- see :meth:`Selector.selects_layer`.

Two rules the parser owes its callers:

* ``+name`` and ``name+`` -- the v0.3 graph operators, "and everything
  upstream/downstream of it" -- are *recognised* and refused by name. v0.3 then
  adds behaviour without changing the grammar or a single call site.
* An unparseable selector raises. Returning nothing would make a typo
  indistinguishable from a project that declares no matching model, and only
  one of those is a mistake.

A selector that parses but matches nothing is not an error: narrowing to
``layer:gold`` in a project with no gold models is an empty plan, which is the
honest answer.
"""

from typing import Any

# The layers `layer:` accepts. Kept here rather than imported from settings:
# this is the selector's vocabulary, and it is what an error message lists.
SELECTABLE_LAYERS = ("bronze", "silver", "gold")

# Recognised, refused, and reserved for v0.3.
GRAPH_OPERATOR = "+"

_ALL = "all"
_LAYER = "layer"
_TAG = "tag"
_NAME = "name"


class SelectorError(ValueError):
    """A selector that cannot be parsed.

    Subclasses ``ValueError`` so a caller already guarding the public API
    against bad input keeps working without learning a new exception.
    """


class Selector:
    """One parsed selector, ready to be matched against models.

    Attributes:
        kind: Which form was parsed -- 'all', 'layer', 'tag' or 'name'
        value: The form's argument; empty for '*'
        source: The selector string as written, for error messages
    """

    def __init__(self, kind: str, value: str, source: str):
        """Store the parsed form.

        Args:
            kind: One of 'all', 'layer', 'tag', 'name'
            value: The argument the form matches on
            source: The original selector text
        """
        self.kind = kind
        self.value = value
        self.source = source

    def matches(self, model: Any) -> bool:
        """Whether one model is in this selection.

        Args:
            model: Anything carrying ``name``, ``layer`` and ``tags``

        Returns:
            True if the model is selected
        """
        if self.kind == _ALL:
            return True

        if self.kind == _LAYER:
            return model.layer == self.value

        if self.kind == _TAG:
            return self.value in (model.tags or [])

        return model.name == self.value

    def selects_layer(self, layer: str) -> bool:
        """Whether any model in ``layer`` could be in this selection.

        Asked *before* the layer is discovered, so it may only answer from the
        selector itself. Only ``layer:`` can: a bare name does not say which
        layer its model lives in, a tag can be carried by a model in any of
        them, and ``*`` wants all three. Those three therefore select every
        layer here and narrow afterwards, in :meth:`matches`.

        This is the pruning half of a selector, and it is deliberately
        conservative -- answering False for a layer that turns out to hold a
        matching model would silently drop it from the plan.

        Args:
            layer: 'bronze', 'silver' or 'gold'

        Returns:
            True unless this is a ``layer:`` selector naming a different layer
        """
        return self.kind != _LAYER or self.value == layer

    def __str__(self) -> str:
        """The selector as it was written."""
        return self.source

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return f"Selector(kind={self.kind!r}, value={self.value!r})"


def parse_selector(selector: str) -> Selector:
    """Parse a selector string into something that can match models.

    Args:
        selector: A selector in the v0.1 grammar. Surrounding whitespace is
            ignored.

    Returns:
        The parsed selector

    Raises:
        SelectorError: If the selector is empty, uses the reserved v0.3 graph
            operators, or names a prefix or layer the grammar does not have.
    """
    text = selector.strip()

    if not text:
        raise SelectorError(
            "An empty selector selects nothing, which is never what a caller "
            "means. Use '*' for every model, 'layer:bronze|silver|gold', "
            "'tag:<value>', or a model's name."
        )

    if text.startswith(GRAPH_OPERATOR) or text.endswith(GRAPH_OPERATOR):
        raise SelectorError(
            f"Selector {text!r} uses the graph operators '+name' / 'name+' "
            f"(a model and everything upstream / downstream of it). They are "
            f"reserved and not yet supported. Select the model by name for "
            f"now: {text.strip(GRAPH_OPERATOR)!r}."
        )

    if text == "*":
        return Selector(_ALL, "", text)

    if ":" in text:
        prefix, _, value = text.partition(":")
        prefix = prefix.strip().lower()
        value = value.strip()

        if not value:
            raise SelectorError(
                f"Selector {text!r} names the {prefix!r} form but gives it no "
                f"value. Write '{prefix}:<value>'."
            )

        if prefix == _LAYER:
            if value.lower() not in SELECTABLE_LAYERS:
                raise SelectorError(
                    f"Unknown layer {value!r} in selector {text!r}. Expected one "
                    f"of {', '.join(SELECTABLE_LAYERS)}."
                )
            return Selector(_LAYER, value.lower(), text)

        if prefix == _TAG:
            return Selector(_TAG, value, text)

        raise SelectorError(
            f"Unknown selector prefix {prefix!r} in {text!r}. The grammar is "
            f"'*', 'layer:<layer>', 'tag:<value>', or a model's name."
        )

    return Selector(_NAME, text, text)


__all__ = ["SELECTABLE_LAYERS", "Selector", "SelectorError", "parse_selector"]
