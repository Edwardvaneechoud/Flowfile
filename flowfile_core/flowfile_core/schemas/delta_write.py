"""Delta write-mode rules shared by the catalog writer and the cloud storage writer.

Stdlib only, so ``cloud_storage_schemas`` and ``input_schema`` can both import it.
"""

MERGE_MODES = frozenset({"upsert", "update", "delete"})


def validate_delta_write_rules(
    write_mode: str,
    merge_keys: list[str],
    track_changes: bool,
    *,
    keyed_modes: frozenset[str] = MERGE_MODES,
    untracked_modes: frozenset[str] = frozenset({"overwrite"}),
) -> None:
    """Raise ``ValueError`` when a Delta write's key columns or change tracking don't fit its mode.

    ``track_changes`` is refused with ``overwrite`` because that write's feed would be every row
    deleted and re-inserted, and the merge modes need key columns to match on. A writer with extra
    modes widens *untracked_modes* / *keyed_modes* (the catalog adds ``scd2`` and ``virtual``);
    every other rule stays in that writer's own validator.
    """
    if track_changes and write_mode in untracked_modes:
        raise ValueError(f"track_changes is not supported with write_mode '{write_mode}'")
    if write_mode in keyed_modes and not merge_keys:
        raise ValueError(f"merge_keys must be non-empty when write_mode is '{write_mode}'")
