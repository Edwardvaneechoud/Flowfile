"""A check for authors of a project kind, to call from their own tests against a test database."""

from __future__ import annotations

from flowfile_core.database.connection import get_db_context
from flowfile_core.project import kinds


def _export(kind: kinds.Kind, owner_id: int) -> dict[str, str]:
    with get_db_context() as db:
        return kind.export(kinds.ExportContext(db=db, owner_id=owner_id))


def assert_kind_round_trips(kind: kinds.Kind, owner_id: int) -> None:
    """Assert the kind's export is stable and its own, and that deleting then applying its entries gives it back.

    It deletes every object of the kind the owner holds and applies them again, so run it on test data only."""
    assert kind.entries is not None and kind.delete is not None and kind.apply is not None, kind.name
    tree = _export(kind, owner_id)
    assert _export(kind, owner_id) == tree, "the export is not deterministic"
    for path in tree:
        owned = kinds.kind_for_path(path)
        assert owned is not None and owned[0].name == kind.name, f"{path!r} is not a path of {kind.name!r}"
    entries = kind.entries(tree)
    assert kind.entries(tree) == entries, "entries of one export differ"
    with get_db_context() as db:
        kind.delete(kinds.ExportContext(db=db, owner_id=owner_id), list(entries))
        db.commit()
    assert kind.entries(_export(kind, owner_id)) == {}, "deleting every key leaves objects in the export"
    with get_db_context() as db:
        kind.apply(kinds.ExportContext(db=db, owner_id=owner_id), entries)
        db.commit()
    assert _export(kind, owner_id) == tree, "applying the entries does not give the export back"
