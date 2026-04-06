"""Integration tests for data contracts via API and service layer.

Covers:
- Contract CRUD through the catalog API
- Validation auto-marks the contract at the latest version
- Successful validation fires downstream table_trigger schedules
- Failed validation does NOT fire downstream triggers
- Contract generation from table profile
- Contract summary status computation
- Update endpoint with definition changes
- Overwrite does NOT fire triggers (moved to validation)
"""

import json
import tempfile

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogNamespace,
    CatalogTable,
    CatalogTableReadLink,
    DataContract,
    FlowFavorite,
    FlowFollow,
    FlowRegistration,
    FlowRun,
    FlowSchedule,
    ScheduleTriggerTable,
)
from flowfile_core.schemas.contract_schema import (
    ColumnContract,
    DataContractDefinition,
    NotNullRule,
    RowCountRule,
    UniqueRule,
    ValueRangeRule,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_auth_token() -> str:
    with TestClient(main.app) as client:
        response = client.post("/auth/token")
        return response.json()["access_token"]


def _get_test_client() -> TestClient:
    token = _get_auth_token()
    c = TestClient(main.app)
    c.headers = {"Authorization": f"Bearer {token}"}
    return c


client = _get_test_client()


def _cleanup_catalog():
    with get_db_context() as db:
        db.query(ScheduleTriggerTable).delete()
        db.query(FlowSchedule).delete()
        db.query(DataContract).delete()
        db.query(CatalogTableReadLink).delete()
        db.query(CatalogTable).delete()
        db.query(FlowFollow).delete()
        db.query(FlowFavorite).delete()
        db.query(FlowRun).delete()
        db.query(FlowRegistration).delete()
        db.query(CatalogNamespace).delete()
        db.commit()


def _make_namespace() -> tuple[int, int]:
    """Create a catalog + schema and return (catalog_id, schema_id)."""
    with get_db_context() as db:
        cat = CatalogNamespace(name="ContractCat", level=0, owner_id=1)
        db.add(cat)
        db.commit()
        db.refresh(cat)
        schema = CatalogNamespace(name="ContractSch", level=1, parent_id=cat.id, owner_id=1)
        db.add(schema)
        db.commit()
        db.refresh(schema)
        return cat.id, schema.id


def _make_table_with_parquet(schema_id: int, data: dict | None = None) -> tuple[int, str]:
    """Create a parquet file, register it in the catalog, return (table_id, path)."""
    if data is None:
        data = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}

    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    df = pl.DataFrame(data)
    df.write_parquet(tmp.name)

    schema_json = json.dumps([{"name": c, "dtype": str(df.dtypes[i])} for i, c in enumerate(df.columns)])

    with get_db_context() as db:
        table = CatalogTable(
            name="test_table",
            namespace_id=schema_id,
            owner_id=1,
            file_path=tmp.name,
            storage_format="parquet",
            schema_json=schema_json,
            row_count=len(df),
            column_count=len(df.columns),
            size_bytes=100,
        )
        db.add(table)
        db.commit()
        db.refresh(table)
        return table.id, tmp.name


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clean_catalog():
    _cleanup_catalog()
    yield
    _cleanup_catalog()


# ---------------------------------------------------------------------------
# Contract CRUD via API
# ---------------------------------------------------------------------------


class TestContractCrudApi:
    def test_create_and_get_contract(self):
        """POST creates a contract, GET retrieves it."""
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)

        resp = client.post(f"/catalog/tables/{table_id}/contract")
        assert resp.status_code == 201
        data = resp.json()
        assert data["table_id"] == table_id
        assert data["status"] == "draft"
        assert data["version"] == 1

        resp = client.get(f"/catalog/tables/{table_id}/contract")
        assert resp.status_code == 200
        assert resp.json()["table_id"] == table_id

    def test_create_contract_409_on_duplicate(self):
        """Creating a second contract for the same table returns 409."""
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)

        resp = client.post(f"/catalog/tables/{table_id}/contract")
        assert resp.status_code == 201

        resp = client.post(f"/catalog/tables/{table_id}/contract")
        assert resp.status_code == 409

    def test_create_contract_404_for_missing_table(self):
        """Creating a contract for a non-existent table returns 404."""
        resp = client.post("/catalog/tables/999999/contract")
        assert resp.status_code == 404

    def test_get_contract_404(self):
        """GET on a table with no contract returns 404."""
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)
        resp = client.get(f"/catalog/tables/{table_id}/contract")
        assert resp.status_code == 404

    def test_delete_contract(self):
        """DELETE removes the contract; subsequent GET returns 404."""
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)

        client.post(f"/catalog/tables/{table_id}/contract")
        resp = client.delete(f"/catalog/tables/{table_id}/contract")
        assert resp.status_code == 204

        resp = client.get(f"/catalog/tables/{table_id}/contract")
        assert resp.status_code == 404

    def test_delete_contract_404(self):
        """DELETE on a non-existent contract returns 404."""
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)
        resp = client.delete(f"/catalog/tables/{table_id}/contract")
        assert resp.status_code == 404

    def test_update_contract_increments_version(self):
        """PUT updates the contract and bumps the version."""
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)

        client.post(f"/catalog/tables/{table_id}/contract")

        resp = client.put(f"/catalog/tables/{table_id}/contract")
        assert resp.status_code == 200
        data = resp.json()
        assert data["version"] == 2


# ---------------------------------------------------------------------------
# Validation + auto-mark
# ---------------------------------------------------------------------------


class TestValidationAutoMark:
    def test_validate_passes_and_marks_contract(self):
        """When all rules pass, the contract is marked as validated."""
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)

        # Create contract with matching rules
        definition = DataContractDefinition(
            columns=[
                ColumnContract(name="id", rules=[NotNullRule()]),
                ColumnContract(name="name", rules=[NotNullRule()]),
                ColumnContract(name="age", rules=[NotNullRule(), ValueRangeRule(min_value=0, max_value=150)]),
            ],
            allow_extra_columns=False,
        )

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.create_contract(
                table_id=table_id,
                name="test_contract",
                owner_id=1,
                definition_json=definition.model_dump_json(),
                status="active",
            )

        resp = client.post(f"/catalog/tables/{table_id}/contract/validate")
        assert resp.status_code == 200
        result = resp.json()
        assert result["passed"] is True
        assert result["total_rows"] == 3

        # Check contract was auto-marked
        resp = client.get(f"/catalog/tables/{table_id}/contract")
        contract = resp.json()
        assert contract["last_validation_passed"] is True
        assert contract["last_validated_at"] is not None

    def test_validate_fails_and_marks_contract(self):
        """When rules fail, the contract is marked as failed."""
        _, schema_id = _make_namespace()
        data = {"id": [1, 2, 3], "name": ["Alice", None, "Charlie"], "age": [25, -5, 35]}
        table_id, _ = _make_table_with_parquet(schema_id, data)

        definition = DataContractDefinition(
            columns=[
                ColumnContract(name="name", rules=[NotNullRule()]),
                ColumnContract(name="age", rules=[ValueRangeRule(min_value=0, max_value=150)]),
            ],
            allow_extra_columns=True,
        )

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.create_contract(
                table_id=table_id,
                name="fail_contract",
                owner_id=1,
                definition_json=definition.model_dump_json(),
                status="active",
            )

        resp = client.post(f"/catalog/tables/{table_id}/contract/validate")
        assert resp.status_code == 200
        result = resp.json()
        assert result["passed"] is False

        # Check contract was auto-marked as failed
        resp = client.get(f"/catalog/tables/{table_id}/contract")
        contract = resp.json()
        assert contract["last_validation_passed"] is False

    def test_validate_without_contract_returns_404(self):
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)

        resp = client.post(f"/catalog/tables/{table_id}/contract/validate")
        assert resp.status_code == 404

    def test_validate_row_count_rule(self):
        """Row count rule is enforced during validation."""
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)

        definition = DataContractDefinition(
            row_count=RowCountRule(min_rows=100),
            allow_extra_columns=True,
        )

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.create_contract(
                table_id=table_id,
                name="row_count_contract",
                owner_id=1,
                definition_json=definition.model_dump_json(),
                status="active",
            )

        resp = client.post(f"/catalog/tables/{table_id}/contract/validate")
        result = resp.json()
        assert result["passed"] is False
        min_row_results = [r for r in result["rule_results"] if r["rule_name"] == "table:min_row_count"]
        assert len(min_row_results) == 1
        assert min_row_results[0]["passed"] is False

    def test_validate_unique_rule_via_api(self):
        """UniqueRule detects duplicate values through the API."""
        _, schema_id = _make_namespace()
        data = {"id": [1, 1, 3], "value": ["a", "b", "c"]}
        table_id, _ = _make_table_with_parquet(schema_id, data)

        definition = DataContractDefinition(
            columns=[ColumnContract(name="id", rules=[UniqueRule()])],
            allow_extra_columns=True,
        )

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.create_contract(
                table_id=table_id,
                name="unique_contract",
                owner_id=1,
                definition_json=definition.model_dump_json(),
                status="active",
            )

        resp = client.post(f"/catalog/tables/{table_id}/contract/validate")
        result = resp.json()
        assert result["passed"] is False
        unique_results = [r for r in result["rule_results"] if r["rule_name"] == "id:unique"]
        assert len(unique_results) == 1
        assert unique_results[0]["violation_count"] == 2

    def test_validate_extra_columns_rejected(self):
        """Extra columns are flagged when allow_extra_columns is False."""
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)

        definition = DataContractDefinition(
            columns=[ColumnContract(name="id", rules=[])],
            allow_extra_columns=False,
        )

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.create_contract(
                table_id=table_id,
                name="extra_cols_contract",
                owner_id=1,
                definition_json=definition.model_dump_json(),
                status="active",
            )

        resp = client.post(f"/catalog/tables/{table_id}/contract/validate")
        result = resp.json()
        assert result["passed"] is False
        unexpected = [r for r in result["rule_results"] if "unexpected" in r["rule_name"]]
        # name, age are unexpected
        assert len(unexpected) == 2


# ---------------------------------------------------------------------------
# Validation triggers downstream
# ---------------------------------------------------------------------------


class TestValidationTriggersDownstream:
    @staticmethod
    def _setup_table_with_trigger(schema_id: int, schedule_type="table_trigger"):
        """Create a table, flow, and schedule. Returns (table_id, flow_id, schedule_id, path)."""
        data = {"id": [1, 2, 3], "name": ["a", "b", "c"]}
        table_id, path = _make_table_with_parquet(schema_id, data)

        with get_db_context() as db:
            flow = FlowRegistration(
                name="downstream_flow",
                flow_path="/tmp/downstream.yaml",
                namespace_id=schema_id,
                owner_id=1,
            )
            db.add(flow)
            db.commit()
            db.refresh(flow)

            schedule = FlowSchedule(
                registration_id=flow.id,
                owner_id=1,
                enabled=True,
                schedule_type=schedule_type,
                trigger_table_id=table_id if schedule_type == "table_trigger" else None,
            )
            db.add(schedule)
            db.commit()
            db.refresh(schedule)

            return table_id, flow.id, schedule.id, path

    def test_successful_validation_fires_downstream_trigger(self, monkeypatch):
        """When validation passes, downstream table_trigger schedules fire."""
        _, schema_id = _make_namespace()
        table_id, flow_id, schedule_id, path = self._setup_table_with_trigger(schema_id)

        monkeypatch.setattr(CatalogService, "_spawn_flow_subprocess", staticmethod(lambda *a, **kw: None))

        # Create a contract that will pass
        definition = DataContractDefinition(
            columns=[
                ColumnContract(name="id", rules=[NotNullRule()]),
                ColumnContract(name="name", rules=[NotNullRule()]),
            ],
            allow_extra_columns=False,
        )

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.create_contract(
                table_id=table_id,
                name="trigger_contract",
                owner_id=1,
                definition_json=definition.model_dump_json(),
                status="active",
            )

        resp = client.post(f"/catalog/tables/{table_id}/contract/validate")
        assert resp.status_code == 200
        assert resp.json()["passed"] is True

        # Verify downstream flow was triggered
        with get_db_context() as db:
            runs = db.query(FlowRun).filter_by(registration_id=flow_id, run_type="scheduled").all()
            assert len(runs) == 1

    def test_failed_validation_does_not_fire_downstream(self, monkeypatch):
        """When validation fails, downstream triggers should NOT fire."""
        _, schema_id = _make_namespace()
        table_id, flow_id, schedule_id, path = self._setup_table_with_trigger(schema_id)

        monkeypatch.setattr(CatalogService, "_spawn_flow_subprocess", staticmethod(lambda *a, **kw: None))

        # Create a contract that will fail (require min 100 rows)
        definition = DataContractDefinition(
            row_count=RowCountRule(min_rows=100),
            allow_extra_columns=True,
        )

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.create_contract(
                table_id=table_id,
                name="failing_contract",
                owner_id=1,
                definition_json=definition.model_dump_json(),
                status="active",
            )

        resp = client.post(f"/catalog/tables/{table_id}/contract/validate")
        assert resp.status_code == 200
        assert resp.json()["passed"] is False

        # Verify no downstream runs were created
        with get_db_context() as db:
            runs = db.query(FlowRun).filter_by(registration_id=flow_id, run_type="scheduled").all()
            assert len(runs) == 0

    def test_overwrite_does_not_fire_triggers(self, monkeypatch):
        """After moving triggers to validation, overwrite_table_data should NOT fire them."""
        _, schema_id = _make_namespace()
        table_id, flow_id, schedule_id, path = self._setup_table_with_trigger(schema_id)

        monkeypatch.setattr(CatalogService, "_spawn_flow_subprocess", staticmethod(lambda *a, **kw: None))

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.overwrite_table_data(table_id, path)

            runs = db.query(FlowRun).filter_by(registration_id=flow_id, run_type="scheduled").all()
            assert len(runs) == 0

    def test_validation_trigger_skips_active_run(self, monkeypatch):
        """If the downstream flow already has an active run, validation should skip it."""
        from datetime import datetime, timezone

        _, schema_id = _make_namespace()
        table_id, flow_id, schedule_id, path = self._setup_table_with_trigger(schema_id)

        monkeypatch.setattr(CatalogService, "_spawn_flow_subprocess", staticmethod(lambda *a, **kw: None))

        # Create an active (unfinished) run
        with get_db_context() as db:
            active_run = FlowRun(
                registration_id=flow_id,
                flow_name="downstream_flow",
                flow_path="/tmp/downstream.yaml",
                user_id=1,
                started_at=datetime.now(timezone.utc),
                number_of_nodes=0,
                run_type="scheduled",
            )
            db.add(active_run)
            db.commit()

        # Create a passing contract
        definition = DataContractDefinition(
            columns=[
                ColumnContract(name="id", rules=[NotNullRule()]),
                ColumnContract(name="name", rules=[NotNullRule()]),
            ],
            allow_extra_columns=False,
        )

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.create_contract(
                table_id=table_id,
                name="skip_contract",
                owner_id=1,
                definition_json=definition.model_dump_json(),
                status="active",
            )

        resp = client.post(f"/catalog/tables/{table_id}/contract/validate")
        assert resp.json()["passed"] is True

        # Only the pre-existing active run, no new one
        with get_db_context() as db:
            runs = db.query(FlowRun).filter_by(registration_id=flow_id, run_type="scheduled").all()
            assert len(runs) == 1

    def test_validation_trigger_updates_schedule_timestamps(self, monkeypatch):
        """After validation fires a trigger, schedule timestamps should be updated."""
        _, schema_id = _make_namespace()
        table_id, flow_id, schedule_id, path = self._setup_table_with_trigger(schema_id)

        monkeypatch.setattr(CatalogService, "_spawn_flow_subprocess", staticmethod(lambda *a, **kw: None))

        definition = DataContractDefinition(
            columns=[
                ColumnContract(name="id", rules=[NotNullRule()]),
                ColumnContract(name="name", rules=[NotNullRule()]),
            ],
            allow_extra_columns=False,
        )

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.create_contract(
                table_id=table_id,
                name="ts_contract",
                owner_id=1,
                definition_json=definition.model_dump_json(),
                status="active",
            )

        resp = client.post(f"/catalog/tables/{table_id}/contract/validate")
        assert resp.json()["passed"] is True

        with get_db_context() as db:
            schedule = db.query(FlowSchedule).get(schedule_id)
            assert schedule.last_triggered_at is not None
            assert schedule.last_trigger_table_updated_at is not None


# ---------------------------------------------------------------------------
# Contract generation
# ---------------------------------------------------------------------------


class TestContractGeneration:
    def test_generate_contract_from_table(self):
        """Auto-generates a contract definition based on table schema."""
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)

        resp = client.post(f"/catalog/tables/{table_id}/contract/generate")
        assert resp.status_code == 200
        definition = resp.json()
        assert "columns" in definition
        col_names = [c["name"] for c in definition["columns"]]
        assert "id" in col_names
        assert "name" in col_names
        assert "age" in col_names

    def test_generate_contract_for_missing_table(self):
        resp = client.post("/catalog/tables/999999/contract/generate")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Contract summary
# ---------------------------------------------------------------------------


class TestContractSummary:
    def test_summary_draft_status(self):
        """A newly created contract should have 'draft' summary status."""
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.create_contract(
                table_id=table_id,
                name="draft_contract",
                owner_id=1,
                definition_json=DataContractDefinition().model_dump_json(),
                status="draft",
            )
            summary = svc.get_contract_summary(table_id)

        assert summary is not None
        assert summary["status"] == "draft"

    def test_summary_failed_status(self):
        """A contract with last_validation_passed=False has 'failed' status."""
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.create_contract(
                table_id=table_id,
                name="fail_contract",
                owner_id=1,
                definition_json=DataContractDefinition().model_dump_json(),
                status="active",
            )
            svc.mark_contract_validated(table_id=table_id, passed=False)
            summary = svc.get_contract_summary(table_id)

        assert summary["status"] == "failed"

    def test_summary_returns_none_without_contract(self):
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            summary = svc.get_contract_summary(table_id)

        assert summary is None

    def test_summary_rule_count(self):
        """Summary should count rules across columns and general rules."""
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)

        definition = DataContractDefinition(
            columns=[
                ColumnContract(name="id", rules=[NotNullRule(), UniqueRule()]),
                ColumnContract(name="name", rules=[NotNullRule()]),
            ],
        )

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.create_contract(
                table_id=table_id,
                name="count_contract",
                owner_id=1,
                definition_json=definition.model_dump_json(),
                status="draft",
            )
            summary = svc.get_contract_summary(table_id)

        assert summary["rule_count"] == 3


# ---------------------------------------------------------------------------
# Service-level contract operations
# ---------------------------------------------------------------------------


class TestContractServiceOps:
    def test_create_and_get_via_service(self):
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            contract = svc.create_contract(
                table_id=table_id,
                name="svc_contract",
                owner_id=1,
                definition_json=DataContractDefinition().model_dump_json(),
            )
            assert contract.table_id == table_id
            assert contract.version == 1

            fetched = svc.get_contract(table_id)
            assert fetched is not None
            assert fetched.id == contract.id

    def test_update_contract_via_service(self):
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)

        definition = DataContractDefinition(
            columns=[ColumnContract(name="id", rules=[NotNullRule()])],
        )

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.create_contract(
                table_id=table_id,
                name="update_me",
                owner_id=1,
                definition_json=DataContractDefinition().model_dump_json(),
            )
            updated = svc.update_contract(
                table_id=table_id,
                name="updated_name",
                definition_json=definition.model_dump_json(),
            )
            assert updated.name == "updated_name"
            assert updated.version == 2

    def test_delete_contract_via_service(self):
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.create_contract(
                table_id=table_id,
                name="delete_me",
                owner_id=1,
                definition_json=DataContractDefinition().model_dump_json(),
            )
            svc.delete_contract(table_id)
            assert svc.get_contract(table_id) is None

    def test_mark_contract_validated_via_service(self):
        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            svc.create_contract(
                table_id=table_id,
                name="mark_me",
                owner_id=1,
                definition_json=DataContractDefinition().model_dump_json(),
                status="active",
            )
            contract = svc.mark_contract_validated(table_id=table_id, passed=True, version=5)
            assert contract.last_validation_passed is True
            assert contract.last_validated_version == 5
            assert contract.last_validated_at is not None

    def test_mark_validated_not_found(self):
        from flowfile_core.catalog.exceptions import ContractNotFoundError

        _, schema_id = _make_namespace()
        table_id, _ = _make_table_with_parquet(schema_id)

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            svc = CatalogService(repo)
            with pytest.raises(ContractNotFoundError):
                svc.mark_contract_validated(table_id=table_id, passed=True)
