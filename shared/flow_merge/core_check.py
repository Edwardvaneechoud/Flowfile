"""``flowfile-merge check --build``: the core check of a flow, for a pipeline that has Flowfile installed.

The only module of the package that imports core, and only when called: importing core starts its catalog database
(``FLOWFILE_STORAGE_DIR``), so a pipeline points that at a scratch folder.
"""

from __future__ import annotations


def check_in_core(document: dict) -> list[str]:
    """The problems core finds in ``document``, one sentence each; ``[]`` when it holds together."""
    from flowfile_core.flowfile.manage.document_check import check_document

    return check_document(document).problems()
