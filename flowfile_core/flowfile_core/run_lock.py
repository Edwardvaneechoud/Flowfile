import asyncio


flow_run_locks = {}


def get_flow_run_lock(flow_id: int) -> asyncio.Lock:
    """Retrieve a lock for the given flow_id, creating it if it doesn't exist."""
    if flow_id not in flow_run_locks:
        flow_run_locks[flow_id] = asyncio.Lock()
    return flow_run_locks[flow_id]
