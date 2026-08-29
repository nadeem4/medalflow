"""The source side of bronze: tables that exist before MedalFlow runs.

Bronze reads from tables it did not create, and this package is the one place
that asks the warehouse what those are. ``LakeDatabase`` lists the tables in a
schema of the configured lake database so introspected bronze discovery can
derive one model per table.

A *declared* bronze model names its source table, so it never reaches this
package at all. That asymmetry is ADR 002, Decision 6: declaration compiles
offline, and introspection is the opt-in mode that does not.
"""
