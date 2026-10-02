"""A notebook kernel's clean run comes back with host paths where a cell wrote the kernel's (no Docker)."""

import copy

from flowfile_core.notebook.validate import host_file_paths

FOLDERS = {"/host/c/Users/me/data": r"C:\Users\me\data"}


def _flow(*nodes: dict) -> dict:
    return {"flowfile_version": "x", "flowfile_id": 1, "flowfile_name": "f", "flowfile_settings": {}, "nodes": list(nodes)}


def _read(node_id: int, path: str) -> dict:
    return {"id": node_id, "type": "read", "setting_input": {"received_file": {"path": path, "file_type": "csv"}}}


def _paths(data: dict) -> list:
    out = []
    for node in data["nodes"]:
        settings = node["setting_input"]
        if "received_file" in settings:
            out.append(settings["received_file"]["path"])
        elif "output_settings" in settings:
            out.append(settings["output_settings"]["directory"])
        elif "cloud_storage_settings" in settings:
            out.append(settings["cloud_storage_settings"]["resource_path"])
        else:
            out.append(settings["path"])
    return out


def test_kernel_paths_become_host_paths():
    data = _flow(
        _read(1, "/host/c/Users/me/data/orders.csv"),
        {
            "id": 2,
            "type": "output",
            "setting_input": {
                "output_settings": {"name": "out.csv", "directory": "/host/c/Users/me/data/out.csv", "file_type": "csv"}
            },
        },
        {"id": 3, "type": "list_files", "setting_input": {"path": "/host/c/Users/me/data"}},
        {
            "id": 4,
            "type": "cloud_storage_reader",
            "setting_input": {"cloud_storage_settings": {"resource_path": "/host/c/Users/me/data/sales"}},
        },
        _read(5, "/host/c/Users/me/data/**/*.csv"),
    )
    before = copy.deepcopy(data)
    assert _paths(host_file_paths(data, FOLDERS)) == [
        r"C:\Users\me\data\orders.csv",
        r"C:\Users\me\data\out.csv",
        r"C:\Users\me\data",
        r"C:\Users\me\data\sales",
        r"C:\Users\me\data\**\*.csv",
    ]
    assert data == before


def test_host_paths_and_uris_are_kept():
    paths = [r"C:\Users\me\data\orders.csv", "/Users/me/data//orders.csv", "/host/c/Users/me/database/x.csv"]
    data = _flow(*(_read(i, path) for i, path in enumerate(paths, 1)))
    data["nodes"].append(
        {"id": 9, "type": "cloud_storage_reader", "setting_input": {"cloud_storage_settings": {"resource_path": "s3://b/x"}}}
    )
    assert _paths(host_file_paths(data, FOLDERS)) == [*paths, "s3://b/x"]
    assert _paths(host_file_paths(_flow(_read(1, "/host/c/Users/me/data/x.csv")))) == ["/host/c/Users/me/data/x.csv"]
