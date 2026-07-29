import pytest
from sqlalchemy import Boolean, Column, Float, Integer, String, Text, create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogNamespace,
    CatalogTable,
    CatalogTableReadLink,
    FlowRegistration,
    FlowSchedule,
)
from flowfile_core.flowfile.flow_graph import FlowGraph, RunInformation, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas

Base = declarative_base()


class Movie(Base):
    __tablename__ = "movies"
    id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    rating = Column(Float)
    votes = Column(Integer)
    description = Column(Text)
    is_active = Column(Boolean, default=True)


class Actor(Base):
    __tablename__ = "actors"
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    birth_year = Column(Integer)


@pytest.fixture
def sqlite_db(tmp_path):
    """Create a temporary SQLite database with sample data."""
    db_path = str(tmp_path / "test.db")
    connection_string = f"sqlite:///{db_path}"
    engine = create_engine(connection_string)
    Base.metadata.create_all(engine)

    session_factory = sessionmaker(bind=engine)
    session: Session = session_factory()

    movies = [
        Movie(id=1, title="The Matrix", rating=8.7, votes=1800000, description="A sci-fi classic", is_active=True),
        Movie(id=2, title="Inception", rating=8.8, votes=2200000, description="Mind-bending thriller", is_active=True),
        Movie(id=3, title="The Shawshank Redemption", rating=9.3, votes=2500000, description="Drama", is_active=True),
    ]
    actors = [
        Actor(id=1, name="Keanu Reeves", birth_year=1964),
        Actor(id=2, name="Leonardo DiCaprio", birth_year=1974),
        Actor(id=3, name="Morgan Freeman", birth_year=1937),
    ]

    session.add_all(movies + actors)
    session.commit()
    session.close()
    engine.dispose()

    yield db_path


@pytest.fixture
def duckdb_db(tmp_path):
    """Temporary DuckDB database mirroring sqlite_db, plus a typed-columns table."""
    import duckdb

    db_path = str(tmp_path / "test.duckdb")
    con = duckdb.connect(db_path)
    con.execute(
        "CREATE TABLE movies (id INTEGER, title VARCHAR, rating DOUBLE, votes BIGINT, "
        "description VARCHAR, is_active BOOLEAN)"
    )
    con.execute(
        "INSERT INTO movies VALUES "
        "(1, 'The Matrix', 8.7, 1800000, 'A sci-fi classic', true), "
        "(2, 'Inception', 8.8, 2200000, 'Mind-bending thriller', true), "
        "(3, 'The Shawshank Redemption', 9.3, 2500000, 'Drama', true)"
    )
    con.execute("CREATE TABLE actors (id INTEGER, name VARCHAR, birth_year INTEGER)")
    con.execute(
        "INSERT INTO actors VALUES "
        "(1, 'Keanu Reeves', 1964), (2, 'Leonardo DiCaprio', 1974), (3, 'Morgan Freeman', 1937)"
    )
    con.execute(
        """
        CREATE TABLE types_zoo AS SELECT
            42::TINYINT AS c_tinyint,
            42::UINTEGER AS c_uinteger,
            42::HUGEINT AS c_hugeint,
            3.140::DECIMAL(12,3) AS c_decimal,
            2.5::DOUBLE AS c_double,
            true AS c_bool,
            'text' AS c_varchar,
            DATE '2024-01-15' AS c_date,
            TIMESTAMP '2024-01-15 10:30:00' AS c_timestamp,
            TIMESTAMPTZ '2024-01-15 10:30:00+00' AS c_timestamptz,
            INTERVAL 3 DAY AS c_interval,
            'abc'::BLOB AS c_blob,
            [1, 2, 3] AS c_list,
            {'a': 1, 'b': 'x'} AS c_struct
        """
    )
    # Single-writer file: release the write connection before tests open their own.
    con.close()

    yield db_path


# Shared catalog test helpers

CATALOG_SAMPLE_DATA = [
    {"name": "Alice", "age": 30, "city": "Amsterdam"},
    {"name": "Bob", "age": 25, "city": "Berlin"},
    {"name": "Charlie", "age": 35, "city": "Copenhagen"},
]


def catalog_cleanup():
    """Remove all catalog / flow-registration rows so tests start clean.

    Also wipes the worker's catalog_virtual_results cache so a previous
    test's ``fvt-{table_id}-{hash}.arrow`` file doesn't satisfy the next
    test's resolve request when fixtures recycle the same table id.
    """
    from shared.storage_config import storage

    with get_db_context() as db:
        db.query(CatalogTableReadLink).delete()
        db.query(FlowSchedule).delete()
        db.query(CatalogTable).delete()
        db.query(FlowRegistration).delete()
        db.query(CatalogNamespace).delete()
        db.commit()

    results_dir = storage.catalog_virtual_results_directory
    if results_dir.exists():
        for entry in results_dir.iterdir():
            if entry.is_file():
                entry.unlink(missing_ok=True)


def create_test_namespace():
    """Create a two-level namespace hierarchy and return the schema-level id."""
    with get_db_context() as db:
        cat = CatalogNamespace(name="TestCat", level=0, owner_id=1)
        db.add(cat)
        db.commit()
        db.refresh(cat)
        schema = CatalogNamespace(name="TestSch", level=1, parent_id=cat.id, owner_id=1)
        db.add(schema)
        db.commit()
        db.refresh(schema)
        return schema.id


def create_test_flow_registration(namespace_id: int, name: str = "test_flow", path: str = "/tmp/test.yaml"):
    """Insert a FlowRegistration row and return its id."""
    with get_db_context() as db:
        reg = FlowRegistration(
            name=name,
            flow_path=path,
            namespace_id=namespace_id,
            owner_id=1,
        )
        db.add(reg)
        db.commit()
        db.refresh(reg)
        return reg.id


def create_test_graph(
    flow_id: int = 1,
    source_registration_id: int | None = None,
    execution_location: str = "local",
) -> FlowGraph:
    """Create a FlowGraph with optional source_registration_id."""
    handler = FlowfileHandler()
    settings = schemas.FlowSettings(
        flow_id=flow_id,
        name="test_flow",
        path=".",
        execution_mode="Development",
        execution_location=execution_location,
        source_registration_id=source_registration_id,
    )
    handler.register_flow(settings)
    return handler.get_flow(flow_id)


def add_test_manual_input(graph: FlowGraph, data: list[dict], node_id: int = 1):
    """Add a manual input node with the given data."""
    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="manual_input")
    graph.add_node_promise(promise)
    manual = input_schema.NodeManualInput(
        flow_id=graph.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData.from_pylist(data),
    )
    graph.add_manual_input(manual)


def add_test_catalog_writer(
    graph: FlowGraph,
    node_id: int,
    depending_on_id: int,
    table_name: str,
    namespace_id: int,
    write_mode: str = "overwrite",
    user_id: int = 1,
):
    """Add a catalog writer node and connect it to its input."""
    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="catalog_writer")
    graph.add_node_promise(promise)
    writer = input_schema.NodeCatalogWriter(
        flow_id=graph.flow_id,
        node_id=node_id,
        depending_on_id=depending_on_id,
        catalog_write_settings=input_schema.CatalogWriteSettings(
            table_name=table_name,
            namespace_id=namespace_id,
            write_mode=write_mode,
        ),
        user_id=user_id,
    )
    graph.add_catalog_writer(writer)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=depending_on_id, to_id=node_id))


def run_test_graph(graph: FlowGraph) -> RunInformation:
    """Run a graph and raise AssertionError on failure."""
    run_info = graph.run_graph()
    if not run_info.success:
        errors = []
        for step in run_info.node_step_result:
            if not step.success:
                errors.append(f"node {step.node_id}: {step.error}")
        raise AssertionError("Graph execution failed:\n" + "\n".join(errors))
    return run_info


@pytest.fixture()
def catalog_clean_state():
    """Fixture to clean catalog state before and after each test."""
    catalog_cleanup()
    yield
    catalog_cleanup()
