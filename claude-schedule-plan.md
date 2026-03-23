Context
Flowfile has a catalog system for registering flows, tracking run history, and managing catalog tables. Currently flows can only be run manually from the designer. This plan adds:

Interval scheduling — run registered flows every N minutes/hours/days
Table-trigger scheduling — auto-run flows when input catalog tables are updated ("any" or "all" mode)
Active run visibility — see all currently running flows (manual + scheduled) in the catalog UI
Cancel support — cancel running flows from the catalog UI

Architecture
flowfile_scheduler (lightweight daemon)
├── Polls DB every N seconds for schedules due to fire
├── Checks interval schedules: now() - last_triggered_at > interval_seconds
├── Checks table triggers: CatalogTable.updated_at > FlowSchedule.last_triggered_at
├── Spawns subprocess per run: `flowfile run flow <path>` (local mode, no worker)
├── Tracks subprocess PIDs for cancellation
└── Updates FlowRun records in DB (start/end/success)

In combined mode (`flowfile run ui`):
  Scheduler runs as asyncio background task in same process
Key design decisions:

Each scheduled run is an isolated subprocess (like flowfile run flow <file> — OFFLOAD_TO_WORKER=False, execution_location="local")
Scheduler is a lightweight daemon — DB polling loop + subprocess management, no HTTP server
Schedule CRUD endpoints live on flowfile_core (it owns the DB)
Scheduler reads DB directly (shared SQLite) — no cross-service HTTP needed


Step 1: Database Model — FlowSchedule
File: flowfile_core/flowfile_core/database/models.py
Add after CatalogTableReadLink:
pythonclass FlowSchedule(Base):
    __tablename__ = "flow_schedules"

    id = Column(Integer, primary_key=True, index=True)
    registration_id = Column(Integer, ForeignKey("flow_registrations.id"), nullable=False)
    schedule_type = Column(String, nullable=False)       # "interval" | "table_trigger"
    interval_seconds = Column(Integer, nullable=True)     # for interval type
    trigger_mode = Column(String, nullable=True)          # "any" | "all" (for table_trigger)
    enabled = Column(Boolean, default=True, nullable=False)
    last_triggered_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
No unique constraint — a flow can have multiple schedules.

Step 2: Modify FlowRun for Active Run Tracking
File: flowfile_core/flowfile_core/routes/routes.py
Problem: _run_and_track() creates FlowRun only AFTER completion — no DB record of running flows.
Solution: Create FlowRun at START with ended_at=None, success=None, UPDATE on completion.
Changes to _run_and_track() (~line 270):

INSERT FlowRun with ended_at=None, success=None before flow.run_graph()
Run flow
UPDATE same record with results

Add run_type="scheduled" alongside existing "full_run" / "fetch_one".
For scheduled runs specifically, the scheduler daemon creates the FlowRun record (with run_type="scheduled", PID stored), then spawns the subprocess. On subprocess exit, it updates the record.

Step 3: Pydantic Schemas
File: flowfile_core/flowfile_core/schemas/catalog_schema.py
pythonclass FlowScheduleCreate(BaseModel):
    registration_id: int
    schedule_type: str           # "interval" | "table_trigger"
    interval_seconds: int | None = None
    trigger_mode: str | None = None  # "any" | "all"

class FlowScheduleUpdate(BaseModel):
    schedule_type: str | None = None
    interval_seconds: int | None = None
    trigger_mode: str | None = None
    enabled: bool | None = None

class FlowScheduleOut(BaseModel):
    id: int
    registration_id: int
    schedule_type: str
    interval_seconds: int | None = None
    trigger_mode: str | None = None
    enabled: bool
    last_triggered_at: datetime | None = None
    created_at: datetime
    updated_at: datetime
    flow_name: str = ""
    flow_path: str = ""
    model_config = {"from_attributes": True}

class ActiveFlowRun(BaseModel):
    id: int
    registration_id: int | None = None
    flow_name: str
    flow_path: str | None = None
    user_id: int
    started_at: datetime
    nodes_completed: int = 0
    number_of_nodes: int = 0
    run_type: str = "full_run"
    model_config = {"from_attributes": True}
```

Update `CatalogStats` to include `total_schedules: int = 0`.

---

## Step 4: Repository Layer

**File:** `flowfile_core/flowfile_core/catalog/repository.py`

Add to Protocol + SQLAlchemy impl:

- `create_schedule(...)` / `get_schedule(id)` / `list_schedules(enabled_only)` / `list_schedules_by_registration(reg_id)`
- `update_schedule(id, **kwargs)` / `delete_schedule(id)` / `update_schedule_last_triggered(id, timestamp)`
- `list_active_runs() -> list[FlowRun]` — `WHERE ended_at IS NULL`

---

## Step 5: Service Layer

**File:** `flowfile_core/flowfile_core/catalog/service.py`

Add to `CatalogService`:

- Schedule CRUD with validation (interval_seconds required for interval, trigger_mode for table_trigger)
- `list_active_runs()` — FlowRuns with `ended_at IS NULL`
- `cancel_active_run(run_id)` — find flow, call cancel (for manual runs); for scheduled runs, kill subprocess by PID

---

## Step 6: Schedule CRUD + Active Run Endpoints

**File:** `flowfile_core/flowfile_core/routes/catalog.py`

### Schedule CRUD
- `POST /catalog/schedules` — create
- `GET /catalog/schedules` — list
- `GET /catalog/schedules/{schedule_id}` — get
- `PUT /catalog/schedules/{schedule_id}` — update
- `DELETE /catalog/schedules/{schedule_id}` — delete
- `POST /catalog/schedules/{schedule_id}/toggle` — enable/disable

### Active Runs
- `GET /catalog/active-runs` — list running flows
- `POST /catalog/active-runs/{run_id}/cancel` — cancel

All with JWT auth.

---

## Step 7: `flowfile_scheduler` Package

**New package:** `flowfile_scheduler/` (top-level, sibling to flowfile_core)
```
flowfile_scheduler/
├── pyproject.toml
├── flowfile_scheduler/
│   ├── __init__.py
│   ├── __main__.py              # Entry: start daemon
│   ├── scheduler.py             # Main polling loop + subprocess management
│   └── config.py                # DB URL, poll interval, flowfile executable path
scheduler.py
pythonclass FlowScheduler:
    def __init__(self, db_session_factory, poll_interval: int = 10):
        self._running = False
        self._poll_interval = poll_interval
        self._active_processes: dict[int, subprocess.Popen]  # run_id -> process

    async def start(self):
        """Main loop."""
        self._running = True
        while self._running:
            self._check_completed_processes()
            await self._check_interval_schedules()
            await self._check_table_trigger_schedules()
            await asyncio.sleep(self._poll_interval)

    async def stop(self):
        self._running = False
        # Optionally terminate active subprocesses

    def _check_completed_processes(self):
        """Poll active subprocesses. For completed ones, update FlowRun
        (ended_at, success based on exit code)."""

    async def _check_interval_schedules(self):
        """Query enabled interval schedules where
        last_triggered_at IS NULL OR now() - last_triggered_at > interval_seconds.
        Trigger each."""

    async def _check_table_trigger_schedules(self):
        """For enabled table_trigger schedules:
        - Join CatalogTableReadLink to find input tables
        - 'any' mode: fire if ANY table.updated_at > schedule.last_triggered_at
        - 'all' mode: fire if ALL tables.updated_at > schedule.last_triggered_at
        Trigger matching schedules."""

    def _trigger_flow(self, registration_id: int, schedule_id: int, flow_path: str):
        """1. Create FlowRun record (ended_at=None, run_type='scheduled')
        2. Spawn: subprocess.Popen(['flowfile', 'run', 'flow', flow_path])
           Or: subprocess.Popen([sys.executable, '-m', 'flowfile', 'run', 'flow', flow_path])
        3. Track PID in _active_processes
        4. Update schedule.last_triggered_at"""

    def cancel_run(self, run_id: int):
        """Kill subprocess by PID, update FlowRun record."""
        proc = self._active_processes.get(run_id)
        if proc:
            proc.terminate()
config.py
pythonDATABASE_URL = os.getenv("FLOWFILE_DATABASE_URL", <same default as core>)
POLL_INTERVAL = int(os.getenv("FLOWFILE_SCHEDULER_POLL_INTERVAL", "10"))
__main__.py
pythondef main():
    scheduler = FlowScheduler(...)
    asyncio.run(scheduler.start())

if __name__ == "__main__":
    main()

Step 8: Combined Mode Integration
File: flowfile/flowfile/web/__init__.py
In extend_app(), after include_worker_routes():
pythondef include_scheduler(app: FastAPI):
    try:
        from flowfile_scheduler.scheduler import FlowScheduler
        scheduler = FlowScheduler(db_session_factory)
        # Start as background task in the event loop
        @app.on_event("startup")
        async def start_scheduler():
            asyncio.create_task(scheduler.start())
        @app.on_event("shutdown")
        async def stop_scheduler():
            await scheduler.stop()
    except ImportError:
        logger.info("flowfile_scheduler not installed, skipping")

Step 9: Enhance flowfile run flow for Scheduled Runs
File: flowfile/flowfile/__main__.py
Add optional --run-id argument to run_flow(). When provided, the subprocess updates the FlowRun record in the DB on completion (success/failure, ended_at, node results). This way the scheduler just spawns the process and the process self-reports its results.
pythondef run_flow(flow_path: str, run_id: int | None = None) -> int:
    # ... existing logic ...
    result = flow.run_graph()
    if run_id:
        # Update FlowRun record in DB with results
        update_flow_run(run_id, result)
    return 0 if result.success else 1

Step 10: Frontend TypeScript Types
File: flowfile_frontend/src/renderer/app/types/catalog.types.ts
typescriptexport interface FlowSchedule {
  id: number;
  registration_id: number;
  schedule_type: "interval" | "table_trigger";
  interval_seconds: number | null;
  trigger_mode: "any" | "all" | null;
  enabled: boolean;
  last_triggered_at: string | null;
  created_at: string;
  updated_at: string;
  flow_name: string;
  flow_path: string;
}

export interface FlowScheduleCreate {
  registration_id: number;
  schedule_type: "interval" | "table_trigger";
  interval_seconds?: number | null;
  trigger_mode?: "any" | "all" | null;
}

export interface FlowScheduleUpdate {
  schedule_type?: string;
  interval_seconds?: number | null;
  trigger_mode?: string | null;
  enabled?: boolean;
}

export interface ActiveFlowRun {
  id: number;
  registration_id: number | null;
  flow_name: string;
  flow_path: string | null;
  user_id: number;
  started_at: string;
  nodes_completed: number;
  number_of_nodes: number;
  run_type: string;
}

// Update CatalogTab
export type CatalogTab = "catalog" | "favorites" | "following" | "runs" | "schedules";

Step 11: Frontend API Client
File: flowfile_frontend/src/renderer/app/api/catalog.api.ts
typescript// Schedules
static async getSchedules(): Promise<FlowSchedule[]>
static async createSchedule(body: FlowScheduleCreate): Promise<FlowSchedule>
static async updateSchedule(id: number, body: FlowScheduleUpdate): Promise<FlowSchedule>
static async deleteSchedule(id: number): Promise<void>
static async toggleSchedule(id: number): Promise<FlowSchedule>

// Active Runs
static async getActiveRuns(): Promise<ActiveFlowRun[]>
static async cancelActiveRun(runId: number): Promise<void>

Step 12: Catalog Store Updates
File: flowfile_frontend/src/renderer/app/stores/catalog-store.ts
Add state: schedules, activeRuns, activeRunsPollingTimer
Add actions: loadSchedules, createSchedule, updateSchedule, deleteSchedule, toggleSchedule, loadActiveRuns, startActiveRunsPolling (3s), stopActiveRunsPolling, cancelActiveRun
Start polling on "schedules" tab, stop when leaving.

Step 13: Schedules Tab UI
New file: flowfile_frontend/src/renderer/app/views/CatalogView/SchedulesPanel.vue
Two sections:
Active Runs — table: Flow Name, Type (manual/scheduled), Started, Progress, Cancel button. Polls every 3s.
Configured Schedules — table: Flow Name, Type, Config ("Every 30 min" / "On any table update"), Enabled toggle, Last Triggered, Edit/Delete. "Create Schedule" button.
New file: flowfile_frontend/src/renderer/app/views/CatalogView/CreateScheduleModal.vue
Modal: flow selector, type radio, conditional config, Save/Cancel.

Step 14: Wire Into CatalogView
File: flowfile_frontend/src/renderer/app/views/CatalogView/CatalogView.vue

Add "Schedules" tab with clock icon + active run count badge
Show SchedulesPanel in detail area

File: flowfile_frontend/src/renderer/app/views/CatalogView/FlowDetailPanel.vue

Add "Schedule" section — show existing schedules or "Add Schedule" button


Files Summary
New package:

flowfile_scheduler/pyproject.toml
flowfile_scheduler/flowfile_scheduler/__init__.py
flowfile_scheduler/flowfile_scheduler/__main__.py
flowfile_scheduler/flowfile_scheduler/scheduler.py
flowfile_scheduler/flowfile_scheduler/config.py

New frontend:

flowfile_frontend/src/renderer/app/views/CatalogView/SchedulesPanel.vue
flowfile_frontend/src/renderer/app/views/CatalogView/CreateScheduleModal.vue

Modified:

flowfile_core/flowfile_core/database/models.py — FlowSchedule model
flowfile_core/flowfile_core/schemas/catalog_schema.py — schedule + active run schemas
flowfile_core/flowfile_core/catalog/repository.py — schedule + active run repo methods
flowfile_core/flowfile_core/catalog/service.py — schedule + active run service methods
flowfile_core/flowfile_core/routes/catalog.py — schedule CRUD + active run endpoints
flowfile_core/flowfile_core/routes/routes.py — FlowRun lifecycle (create at start, update on end)
flowfile/flowfile/__main__.py — add --run-id for self-reporting scheduled runs
flowfile/flowfile/web/__init__.py — include_scheduler for combined mode
flowfile_frontend/src/renderer/app/types/catalog.types.ts — TS types
flowfile_frontend/src/renderer/app/api/catalog.api.ts — API methods
flowfile_frontend/src/renderer/app/stores/catalog-store.ts — state + actions
flowfile_frontend/src/renderer/app/views/CatalogView/CatalogView.vue — schedules tab
flowfile_frontend/src/renderer/app/views/CatalogView/FlowDetailPanel.vue — schedule section


Verification

Scheduler tests: flowfile_scheduler/tests/ — interval check logic, table trigger logic, subprocess spawning (mock)
Core tests: flowfile_core/tests/test_catalog.py — schedule CRUD, active runs query
Manual testing:

Start core + scheduler (or combined via flowfile run ui)
Register a flow, create interval schedule (60s), verify subprocess spawns and flow executes
Create table-trigger schedule, update catalog table, verify flow triggers
Check Schedules tab shows active runs, cancel a running flow (subprocess killed)


Regression: poetry run pytest flowfile_core/tests