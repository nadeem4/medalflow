"""A project whose gold layer declares one name twice.

Discovery keys on `name`, so a duplicate is fatal to the whole layer -- it
cannot say which of the two models the name refers to. `compile()` has to
report that as an error rather than raise, and has to keep compiling the other
layers.

Gold carries the duplicate because gold is never filtered by
`configured_models`, so nothing else can influence what the walk finds.
"""
