"""A notebook kernel's clean run comes back with its file paths as the cells wrote them (the kernel mounts no host
folder); core recomputes the absolute paths on this machine (no Docker)."""

import copy
from pathlib import PurePosixPath

from flowfile_core.notebook.validate import host_file_paths


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


def test_paths_stay_as_written_and_absolute_paths_are_recomputed_here(tmp_path):
    csv = tmp_path / "orders.csv"
    csv.write_text("a\n1\n")
    data = _flow(
        _read(1, str(csv)),
        _read(2, r"C:\Users\me\data\orders.csv"),
        {
            "id": 3,
            "type": "output",
            "setting_input": {"output_settings": {"name": "out.csv", "directory": str(tmp_path), "file_type": "csv"}},
        },
        {"id": 4, "type": "list_files", "setting_input": {"path": str(tmp_path)}},
        {"id": 5, "type": "cloud_storage_reader", "setting_input": {"cloud_storage_settings": {"resource_path": "s3://b/x"}}},
    )
    before = copy.deepcopy(data)
    out = host_file_paths(data)
    assert _paths(out) == [str(csv), r"C:\Users\me\data\orders.csv", str(tmp_path), str(tmp_path), "s3://b/x"]
    assert out["nodes"][0]["setting_input"]["received_file"]["abs_file_path"] == str(csv.resolve())
    assert data == before, "the kernel's data is copied, never changed in place"


def test_a_relative_windows_path_named_by_a_linux_kernel_is_not_joined_twice(tmp_path, monkeypatch):
    """The Linux kernel names ``titanic\\train.csv`` by its whole path (``\\`` is no separator there), so the host
    must not append that name to the path it already ends in."""
    monkeypatch.chdir(tmp_path)
    read, mixed, written = r"titanic\train.csv", r"titanic/sub\train.csv", r"out\x.csv"
    data = _flow(
        {
            "id": 1,
            "type": "read",
            "setting_input": {"received_file": {"path": read, "name": PurePosixPath(read).name, "file_type": "csv"}},
        },
        {
            "id": 2,
            "type": "read",
            "setting_input": {"received_file": {"path": mixed, "name": PurePosixPath(mixed).name, "file_type": "csv"}},
        },
        {
            "id": 3,
            "type": "output",
            "setting_input": {
                "output_settings": {
                    "name": PurePosixPath(written).name,
                    "directory": written,
                    "file_type": "csv",
                    "table_settings": {"file_type": "csv"},
                }
            },
        },
    )
    out = host_file_paths(data)
    assert [node["setting_input"].get("received_file", {}).get("abs_file_path") for node in out["nodes"][:2]] == [
        str((tmp_path / read).resolve()),
        str((tmp_path / mixed).resolve()),
    ]
    assert out["nodes"][2]["setting_input"]["output_settings"]["abs_file_path"] == str((tmp_path / written).resolve())
