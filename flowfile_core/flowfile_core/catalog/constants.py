"""Defaults, limits and seeded-namespace names for the catalog domain.

Centralised so call sites read intent rather than literals.

Alembic migrations deliberately inline these strings instead of importing them:
a migration is a historical record of one schema state and must keep doing in
2027 exactly what it did when it was written, even if a name changes here.
"""

from dataclasses import dataclass

DEFAULT_PREVIEW_LIMIT = 100
MAX_PREVIEW_LIMIT = 1000
MAX_VISUALIZATION_ROWS = 500_000
DEFAULT_VISUALIZATION_ROWS = 100_000
MAX_THUMBNAIL_BYTES = 500_000
DEFAULT_SQL_MAX_ROWS = 10_000
MIN_SCHEDULE_INTERVAL_SECONDS = 60
QUERY_VIRTUAL_TABLE_RECURSION_LIMIT = 5

# save_sql_query_as_flow node-layout positions
SAVED_FLOW_NODE_X = 100
SAVED_FLOW_NODE_Y_BASE = 100
SAVED_FLOW_NODE_Y_STEP = 150
SAVED_FLOW_SQL_NODE_X = 400
SAVED_FLOW_SQL_NODE_Y = 200


@dataclass(frozen=True)
class SeededNamespace:
    """A namespace the application creates and manages, never the user."""

    name: str
    description: str


ROOT_CATALOG = SeededNamespace("General", "Default catalog")
DEFAULT_SCHEMA = SeededNamespace("default", "Default schema for user flows")
UNNAMED_FLOWS = SeededNamespace("Unnamed Flows", "Quick-created flows that have not yet been named")
LOCAL_FLOWS = SeededNamespace("Local Flows", "Flows saved to disk at user-chosen paths")
PYTHON_EDITOR_FLOWS = SeededNamespace("Python Editor", "Flows authored programmatically via the FlowFrame Python API")
KAFKA_SYNC = SeededNamespace("sync", "Kafka sync flows")

# Children of ROOT_CATALOG created by init_db on every install, in this order.
SEEDED_SCHEMAS: tuple[SeededNamespace, ...] = (DEFAULT_SCHEMA, UNNAMED_FLOWS, LOCAL_FLOWS)

# App-managed schemas under ROOT_CATALOG that are never offered as a user save
# target. Mirrors SYSTEM_NAMESPACE_NAMES in the frontend's types/catalog.types.ts.
SYSTEM_SCHEMA_NAMES: frozenset[str] = frozenset(
    {UNNAMED_FLOWS.name, LOCAL_FLOWS.name, PYTHON_EDITOR_FLOWS.name, KAFKA_SYNC.name}
)
