"""Run a chain with deterministic DDL ordering for raw sqlite3 .schema comparisons.

SQLAlchemy stores indexes/constraints in identity-hashed sets, so independent
runs otherwise reorder equivalent DDL. Only this probe stabilizes their order;
no SQL text or schema dump is normalized.
"""

import sys

from alembic import command
from alembic.config import Config
from alembic.operations.batch import ApplyBatchImpl
from sqlalchemy import Table, event


def constraint_key(constraint):
    return (type(constraint).__name__, constraint.name or "", tuple(c.name for c in constraint.columns))


Table._sorted_constraints = property(lambda table: sorted(table.constraints, key=constraint_key))


class OrderedIndexes(set):
    def __iter__(self):
        return iter(sorted(super().__iter__(), key=lambda index: index.name or ""))


@event.listens_for(Table, "before_create")
def order_indexes(table, connection, **kw):
    table.indexes = OrderedIndexes(table.indexes)


_gather_indexes = ApplyBatchImpl._gather_indexes_from_both_tables
ApplyBatchImpl._gather_indexes_from_both_tables = lambda batch: sorted(
    _gather_indexes(batch), key=lambda index: index.name or ""
)


cfg = Config()
cfg.set_main_option("script_location", sys.argv[1])
command.upgrade(cfg, "head")
