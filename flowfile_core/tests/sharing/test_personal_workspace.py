"""Each person on a server browses, saves and opens files in a folder of their own, beside a shared folder.

The conftest puts the process in docker mode with ``tmp_path`` as the user data folder.
"""

import os
from pathlib import Path

import pytest

from flowfile_core.fileExplorer import workspace
from shared.storage_config import storage

REFUSED = "is not in your workspace or the shared folder"


@pytest.fixture
def alice(client_for):
    return client_for("alice")


@pytest.fixture
def bob(client_for):
    return client_for("bob")


def _workspace(client) -> Path:
    response = client.get("/files/default_path/")
    assert response.status_code == 200, response.text
    return Path(response.json())


def _alice_file(alice) -> Path:
    folder = _workspace(alice)
    (folder / "private.csv").write_text("a,b\n1,2\n")
    return folder / "private.csv"


def _new_flow(client, path: Path) -> int:
    response = client.post(
        "/editor/create_flow/", params={"flow_path": str(path), "register_in_catalog": False, "persist": True}
    )
    assert response.status_code == 200, response.text
    return response.json()


class TestRule:
    def test_the_folder_is_named_by_the_user_id_and_made_on_first_use(self, tmp_path, users):
        folder = workspace.workspace_of(users["alice"].id)
        assert folder == tmp_path / "workspaces" / str(users["alice"].id)
        assert folder.is_dir()

    def test_another_persons_folder_is_not_a_place_of_mine(self, users):
        theirs = workspace.workspace_of(users["alice"].id) / "x.csv"
        assert workspace.data_place(str(theirs), users["bob"].id) is None
        assert workspace.data_place(str(theirs), users["alice"].id) == str(theirs)

    def test_dot_dot_and_a_symlink_out_are_resolved_before_the_check(self, users):
        mine = workspace.workspace_of(users["bob"].id)
        theirs = workspace.workspace_of(users["alice"].id)
        (theirs / "x.csv").write_text("a\n1\n")
        (mine / "link.csv").symlink_to(theirs / "x.csv")
        assert workspace.data_place(str(mine / ".." / theirs.name / "x.csv"), users["bob"].id) is None
        assert workspace.data_place(str(mine / "link.csv"), users["bob"].id) is None

    def test_the_shared_folder_holds_no_workspace(self, tmp_path, users):
        (tmp_path / "team.csv").write_text("a\n1\n")
        assert workspace.data_place(str(tmp_path / "team.csv"), users["bob"].id) is not None
        assert workspace.data_place(str(tmp_path / "workspaces"), users["bob"].id) is None

    def test_the_storage_folder_inside_the_shared_folder_is_left_out(self, tmp_path, monkeypatch, users):
        monkeypatch.setattr(storage, "_base_dir", tmp_path / "internal")
        (tmp_path / "internal").mkdir()
        assert workspace.data_place(str(tmp_path / "internal" / "key.json"), users["bob"].id) is None

    def test_without_a_person_only_the_shared_folder(self, tmp_path, users):
        theirs = workspace.workspace_of(users["alice"].id)
        assert workspace.data_place(str(theirs / "x.csv"), None) is None
        assert workspace.data_place(str(tmp_path / "x.csv"), None) is not None


class TestBrowsing:
    def test_each_person_starts_in_their_own_folder(self, alice, bob, users, tmp_path):
        assert _workspace(alice) == tmp_path / "workspaces" / str(users["alice"].id)
        assert _workspace(bob) == tmp_path / "workspaces" / str(users["bob"].id)

    def test_the_roots_are_the_workspace_and_shared(self, alice, tmp_path):
        roots = alice.get("/files/roots").json()
        assert roots == [
            {"name": "Workspace", "path": str(_workspace(alice))},
            {"name": "Shared", "path": str(tmp_path)},
        ]

    def test_a_person_does_not_see_another_persons_files(self, alice, bob):
        private = _alice_file(alice)
        own = bob.get("/files/directory_contents/", params={"directory": str(_workspace(bob))})
        assert own.status_code == 200
        assert private.name not in [f["name"] for f in own.json()]
        theirs = bob.get("/files/directory_contents/", params={"directory": str(private.parent)})
        assert theirs.status_code == 403

    @pytest.mark.parametrize("route", ["/files/directory_contents/", "/files/files_in_local_directory/"])
    def test_dot_dot_absolute_and_symlink_are_refused(self, alice, bob, route):
        theirs = _alice_file(alice).parent
        mine = _workspace(bob)
        (mine / "into_alice").symlink_to(theirs, target_is_directory=True)
        for directory in (str(theirs), str(mine / ".." / theirs.name), str(mine / "into_alice")):
            response = bob.get(route, params={"directory": directory})
            assert response.status_code == 403, (directory, response.text)

    def test_a_symlink_out_is_not_listed(self, alice, bob):
        theirs = _alice_file(alice)
        mine = _workspace(bob)
        (mine / "peek.csv").symlink_to(theirs)
        listed = bob.get("/files/directory_contents/", params={"directory": str(mine)}).json()
        assert "peek.csv" not in [f["name"] for f in listed]

    def test_the_shared_folder_lists_no_workspaces(self, alice, tmp_path):
        _workspace(alice)
        (tmp_path / "team.csv").write_text("a\n1\n")
        listed = alice.get("/files/directory_contents/", params={"directory": str(tmp_path)})
        assert listed.status_code == 200
        names = [f["name"] for f in listed.json()]
        assert "team.csv" in names and "workspaces" not in names
        assert (
            alice.get("/files/directory_contents/", params={"directory": str(tmp_path / "workspaces")}).status_code
            == 403
        )

    def test_a_folder_is_made_only_in_a_place_of_ones_own(self, alice, bob):
        theirs = _alice_file(alice).parent
        made = bob.post("/files/create_directory", json={"source_path": str(_workspace(bob)), "dir_name": "churn"})
        assert made.status_code == 200 and (_workspace(bob) / "churn").is_dir()
        refused = bob.post("/files/create_directory", json={"source_path": str(theirs), "dir_name": "planted"})
        assert refused.status_code == 403
        assert not (theirs / "planted").exists()

    def test_flow_files_of_another_are_not_listed(self, alice, bob):
        theirs = _workspace(alice)
        (theirs / "old.flowfile").write_text("x")
        assert [f["name"] for f in alice.get("/files/available_flow_files", params={"path": str(theirs)}).json()] == [
            "old.flowfile"
        ]
        assert bob.get("/files/available_flow_files", params={"path": str(theirs)}).json() == []

    def test_a_refused_folder_names_the_path_in_one_sentence(self, alice, bob):
        theirs = _workspace(alice)
        for response in (
            bob.get("/files/directory_contents/", params={"directory": str(theirs)}),
            bob.get("/files/files_in_local_directory/", params={"directory": str(theirs)}),
            bob.post("/files/create_directory", json={"source_path": str(theirs), "dir_name": "x"}),
            bob.get("/files/download", params={"path": str(theirs / "a.csv")}),
            bob.post(
                "/file_manager/upload", params={"directory": str(theirs)}, files={"file": ("a.csv", b"a\n", "text/csv")}
            ),
        ):
            assert response.status_code == 403, response.text
            assert response.json()["detail"].endswith(REFUSED + "."), response.text


class TestTheServersOwnFolders:
    def test_the_working_folder_inside_the_shared_folder_is_left_out(self, tmp_path, monkeypatch, users):
        (tmp_path / "node").mkdir()
        monkeypatch.chdir(tmp_path / "node")
        assert workspace.data_place(str(tmp_path / "node" / "node.env"), users["bob"].id) is None
        assert workspace.data_place(str(tmp_path / "team.csv"), users["bob"].id) is not None

    def test_the_secure_storage_folder_inside_the_shared_folder_is_left_out(self, tmp_path, monkeypatch, users):
        monkeypatch.setenv("SECURE_STORAGE_PATH", str(tmp_path / "secure"))
        (tmp_path / "secure").mkdir()
        assert workspace.data_place(str(tmp_path / "secure" / ".secret_key"), users["bob"].id) is None

    @pytest.mark.parametrize(
        "folder",
        ["ai_sessions", "catalog_tables", "catalog_virtual_results", "notebooks", "projects", "user_defined_nodes"],
    )
    def test_the_apps_own_folders_are_not_in_the_shared_folder(self, alice, users, tmp_path, folder):
        (tmp_path / folder / "7").mkdir(parents=True)
        (tmp_path / folder / "7" / "x.json").write_text("{}")
        names = [f["name"] for f in alice.get("/files/directory_contents/", params={"directory": str(tmp_path)}).json()]
        assert folder not in names
        assert alice.get("/files/download", params={"path": str(tmp_path / folder / "7" / "x.json")}).status_code == 403
        upload = alice.post(
            "/file_manager/upload",
            params={"directory": str(tmp_path / folder)},
            files={"file": ("x.csv", b"a\n1\n", "text/csv")},
        )
        assert upload.status_code == 403 and not (tmp_path / folder / "x.csv").exists()
        made = alice.post("/files/create_directory", json={"source_path": str(tmp_path / folder), "dir_name": "d"})
        assert made.status_code == 403 and not (tmp_path / folder / "d").exists()
        assert workspace.data_place(str(tmp_path / folder / "x.csv"), users["alice"].id) is None

    @pytest.mark.parametrize("folder", ["flows", "uploads", "outputs"])
    def test_the_flows_uploads_and_outputs_folders_are_browsed_in_the_shared_folder(self, alice, tmp_path, folder):
        (tmp_path / folder).mkdir(exist_ok=True)
        (tmp_path / folder / "x.csv").write_text("a\n1\n")
        names = [f["name"] for f in alice.get("/files/directory_contents/", params={"directory": str(tmp_path)}).json()]
        assert folder in names
        assert alice.get("/files/download", params={"path": str(tmp_path / folder / "x.csv")}).status_code == 200

    def test_a_file_node_uses_the_uploads_and_outputs_folders_and_not_the_flows_folder(self, users, tmp_path):
        uid = users["alice"].id
        assert workspace.data_place(str(storage.uploads_directory / "x.csv"), uid) is not None
        assert workspace.data_place(str(storage.outputs_directory / "x.csv"), uid) is not None
        assert workspace.data_place(str(storage.flows_directory / "x.csv"), uid) is None
        assert workspace.flow_file_place(str(storage.flows_directory / "x.yaml"), uid) is not None


@pytest.mark.skipif(
    not Path(str(Path.cwd()).swapcase()).exists() and not Path(str(Path.home()).swapcase()).exists(),
    reason="the disk here tells letter case apart",
)
class TestLetterCase:
    def test_another_persons_folder_spelled_in_other_letters_is_refused(self, alice, bob, users, tmp_path):
        private = _alice_file(alice)
        shouted = tmp_path / "WORKSPACES" / private.parent.name
        assert shouted.exists()
        assert bob.get("/files/directory_contents/", params={"directory": str(shouted)}).status_code == 403
        assert bob.get("/files/download", params={"path": str(shouted / private.name)}).status_code == 403
        assert workspace.data_place(str(shouted / private.name), users["bob"].id) is None
        upload = bob.post(
            "/file_manager/upload", params={"directory": str(shouted)}, files={"file": ("p.csv", b"a\n", "text/csv")}
        )
        assert upload.status_code == 403 and not (private.parent / "p.csv").exists()
        made = bob.post("/files/create_directory", json={"source_path": str(shouted), "dir_name": "planted"})
        assert made.status_code == 403 and not (private.parent / "planted").exists()

    def test_the_apps_own_folders_spelled_in_other_letters_are_refused_and_not_listed(
        self, alice, bob, users, tmp_path
    ):
        private = _alice_file(alice)
        (tmp_path / "notebooks" / "7").mkdir(parents=True)
        (tmp_path / "notebooks" / "7" / "cells.yaml").write_text("a: 1\n")
        uid = users["bob"].id
        for spelled in (tmp_path / "NOTEBOOKS" / "7" / "cells.yaml", tmp_path / "Workspaces" / private.parent.name):
            assert spelled.exists()
            for place in (workspace.data_place, workspace.flow_file_place, workspace.file_folder):
                assert place(str(spelled), uid) is None, spelled
        pattern = str(tmp_path / "WORKSPACES" / "*" / "*.csv")
        with pytest.raises(workspace.NotYours):
            workspace.require_data_paths(uid, pattern, expand_glob=True)
        listed = workspace.explorer_for(str(tmp_path), uid, None).list_contents(recursive=True, show_hidden=True)
        assert not [entry.path for entry in listed if "workspaces" in str(entry.path).lower()]
        assert not [entry.path for entry in listed if "notebooks" in str(entry.path).lower()]


class TestRegisteringAFlow:
    def test_a_flow_is_not_registered_at_another_persons_path(self, alice, bob):
        theirs = _workspace(alice) / "churn.yaml"
        _new_flow(alice, theirs)
        before = theirs.read_text()
        refused = bob.post("/flow/register/", json={"flow_id": 987654, "name": "x", "path": str(theirs)})
        assert refused.status_code == 403
        assert REFUSED in refused.json()["detail"]
        assert bob.post("/editor/rename_flow/", json={"flow_id": 987654, "name": "mine"}).status_code == 404
        assert theirs.read_text() == before

    def test_a_flow_is_registered_at_ones_own_path_and_its_id_answered(self, bob):
        own = _workspace(bob) / "mine.yaml"
        registered = bob.post("/flow/register/", json={"flow_id": 987656, "name": "mine", "path": str(own)})
        assert registered.status_code == 200, registered.text
        assert registered.json() == 987656
        bob.post("/editor/close_flow/", params={"flow_id": 987656})


class TestProjectExport:
    def test_a_project_is_not_written_into_the_workspaces_folder(self, bob, tmp_path):
        flow_id = _new_flow(bob, _workspace(bob) / "workspaces.yaml")
        bob.post(
            "/editor/add_node/",
            params={"flow_id": flow_id, "node_id": 1, "node_type": "manual_input", "pos_x": 0, "pos_y": 0},
        )
        before = sorted(p.name for p in (tmp_path / "workspaces").iterdir())
        refused = bob.post(
            "/editor/code_to_project/save",
            json={"flow_id": flow_id, "target_directory": str(tmp_path), "overwrite": True},
        )
        assert refused.status_code == 403, refused.text
        assert sorted(p.name for p in (tmp_path / "workspaces").iterdir()) == before


class TestDeletedPerson:
    def test_a_deleted_persons_folder_is_set_aside_and_a_new_account_starts_empty(self, client_for, users, tmp_path):
        carol = client_for("carol")
        folder = _workspace(carol)
        (folder / "left.csv").write_text("a\n1\n")
        deleted = client_for("admin").delete(f"/auth/users/{users['carol'].id}")
        assert deleted.status_code == 200, deleted.text
        assert not folder.exists()
        [aside] = (tmp_path / "workspaces" / workspace.DELETED_FOLDER).iterdir()
        assert aside.name.startswith(f"{users['carol'].id}-")
        assert (aside / "left.csv").exists()
        assert list(workspace.workspace_of(users["carol"].id).iterdir()) == []
        assert workspace.data_place(str(aside / "left.csv"), users["carol"].id) is None

    def test_an_account_whose_folder_cannot_be_set_aside_is_kept_with_its_folder(self, client_for, users, tmp_path):
        from flowfile_core.database import models as db_models
        from flowfile_core.database.connection import get_db_context

        carol = client_for("carol")
        folder = _workspace(carol)
        (folder / "left.csv").write_text("a\n1\n")
        (tmp_path / "workspaces" / workspace.DELETED_FOLDER).write_text("a file where the folder would be made")
        deleted = client_for("admin").delete(f"/auth/users/{users['carol'].id}")
        assert deleted.status_code == 500, deleted.text
        assert "kept" in deleted.json()["detail"]
        assert (folder / "left.csv").exists()
        with get_db_context() as db:
            assert db.get(db_models.User, users["carol"].id) is not None
        assert carol.get("/files/default_path/").status_code == 200


@pytest.fixture(params=["docker", "server"])
def server_kind(request, monkeypatch):
    from flowfile_core.configs import settings

    monkeypatch.setenv("FLOWFILE_MODE", request.param)
    monkeypatch.setattr(settings, "FLOWFILE_MODE", request.param)
    return request.param


class TestBothKindsOfServer:
    def test_a_person_works_in_their_own_workspace(self, alice, server_kind):
        import polars as pl

        own = _workspace(alice)
        flow_id = _new_flow(alice, own / "churn.yaml")
        assert alice.post("/save_flow", params={"flow_id": flow_id, "register_in_catalog": False}).status_code == 200
        assert alice.get("/import_flow/", params={"flow_path": str(own / "churn.yaml")}).status_code == 200
        made = alice.post("/files/create_directory", json={"source_path": str(own), "dir_name": "churn"})
        assert made.status_code == 200, made.text
        pl.DataFrame({"a": [1]}).write_excel(own / "book.xlsx")
        sheets = alice.get("/api/get_xlsx_sheet_names", params={"path": str(own / "book.xlsx")})
        assert sheets.status_code == 200, sheets.text
        upload = alice.post(
            "/file_manager/upload", params={"directory": str(own)}, files={"file": ("c.csv", b"a\n1\n", "text/csv")}
        )
        assert upload.status_code == 200, upload.text
        assert alice.get("/files/download", params={"path": str(own / "c.csv")}).status_code == 200

    def test_another_persons_workspace_stays_closed(self, alice, bob, server_kind):
        theirs = _alice_file(alice)
        assert bob.get("/files/directory_contents/", params={"directory": str(theirs.parent)}).status_code == 403
        assert bob.get("/files/download", params={"path": str(theirs)}).status_code == 403
        created = bob.post(
            "/editor/create_flow/", params={"flow_path": str(theirs.parent / "x.yaml"), "register_in_catalog": False}
        )
        assert created.status_code == 403


class TestAServerRunByHand:
    def test_the_flows_uploads_and_outputs_folders_inside_the_storage_folder_are_browsed(
        self, alice, users, tmp_path, monkeypatch
    ):
        from flowfile_core.configs import settings

        monkeypatch.setenv("FLOWFILE_MODE", "server")
        monkeypatch.setattr(settings, "FLOWFILE_MODE", "server")
        monkeypatch.setattr(storage, "_base_dir", tmp_path / ".flowfile")
        for folder in (storage.uploads_directory, storage.outputs_directory, storage.flows_directory):
            folder.mkdir(parents=True, exist_ok=True)
        (storage.outputs_directory / "result.csv").write_text("a\n1\n")
        names = [root["name"] for root in alice.get("/files/roots").json()]
        assert names[:2] == ["Workspace", "Shared"] and {"Uploads", "Outputs", "Flows"} <= set(names)
        for folder in (storage.uploads_directory, storage.outputs_directory, storage.flows_directory):
            listed = alice.get("/files/directory_contents/", params={"directory": str(folder)})
            assert listed.status_code == 200, (folder, listed.text)
        got = alice.get("/files/download", params={"path": str(storage.outputs_directory / "result.csv")})
        assert got.status_code == 200
        hidden = alice.get("/files/directory_contents/", params={"directory": str(tmp_path), "include_hidden": True})
        assert ".flowfile" not in [f["name"] for f in hidden.json()]


class TestAServerInTheAccountsHome:
    """A server run by hand keeps user data in the home of the account it runs as: there is no shared folder."""

    @pytest.fixture
    def home(self, alice, tmp_path, monkeypatch):
        from flowfile_core.configs import settings

        monkeypatch.setenv("FLOWFILE_MODE", "server")
        monkeypatch.setattr(settings, "FLOWFILE_MODE", "server")
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setattr(storage, "_base_dir", tmp_path / ".flowfile")
        (tmp_path / ".ssh").mkdir()
        (tmp_path / ".ssh" / "id_rsa").write_text("PRIVATE KEY")
        (tmp_path / ".config" / "tool").mkdir(parents=True)
        (tmp_path / ".config" / "tool" / "settings.yaml").write_text("token: abc\n")
        for folder in (storage.uploads_directory, storage.outputs_directory, storage.flows_directory):
            folder.mkdir(parents=True, exist_ok=True)
        return tmp_path

    def test_the_roots_are_the_workspace_and_the_flow_folders(self, alice, home):
        assert [root["name"] for root in alice.get("/files/roots").json()] == [
            "Workspace",
            "Flows",
            "Uploads",
            "Outputs",
        ]
        assert _workspace(alice).parent == home / "workspaces"

    @pytest.mark.parametrize("spelling", ["{home}/.config/tool/settings.yaml", "~/.config/tool/settings.yaml"])
    def test_a_flow_is_not_created_over_a_file_of_the_home(self, alice, home, spelling):
        target = home / ".config" / "tool" / "settings.yaml"
        created = alice.post(
            "/editor/create_flow/",
            params={"flow_path": spelling.format(home=home), "register_in_catalog": False, "persist": True},
        )
        assert created.status_code == 403, created.text
        assert target.read_text() == "token: abc\n"

    @pytest.mark.parametrize("spelling", ["{home}/.ssh/id_rsa", "~/.ssh/id_rsa"])
    def test_a_file_of_the_home_is_not_downloaded(self, alice, home, spelling):
        got = alice.get("/files/download", params={"path": spelling.format(home=home)})
        assert got.status_code == 403, got.text
        assert b"PRIVATE KEY" not in got.content
        assert got.json()["detail"].endswith("is not in your workspace.")

    @pytest.mark.parametrize("spelling", ["{home}/.ssh", "~/.ssh"])
    def test_nothing_is_uploaded_into_a_folder_of_the_home(self, alice, home, spelling):
        upload = alice.post(
            "/file_manager/upload",
            params={"directory": spelling.format(home=home)},
            files={"file": ("x.csv", b"a\n", "text/csv")},
        )
        assert upload.status_code == 403, upload.text
        assert sorted(p.name for p in (home / ".ssh").iterdir()) == ["id_rsa"]

    def test_the_home_is_not_listed_read_or_written(self, alice, users, home):
        for directory in (home, home / ".ssh", home / ".config"):
            for route in ("/files/directory_contents/", "/files/files_in_local_directory/"):
                assert alice.get(route, params={"directory": str(directory)}).status_code == 403, directory
        made = alice.post("/files/create_directory", json={"source_path": str(home), "dir_name": "planted"})
        assert made.status_code == 403 and not (home / "planted").exists()
        opened = alice.get("/import_flow/", params={"flow_path": str(home / ".config" / "tool" / "settings.yaml")})
        assert opened.status_code == 403
        assert alice.get("/api/get_xlsx_sheet_names", params={"path": str(home / "book.xlsx")}).status_code == 403
        uid = users["alice"].id
        for place in (workspace.data_place, workspace.flow_file_place, workspace.file_folder):
            assert place(str(home / ".ssh" / "id_rsa"), uid) is None
            assert place(str(home / "notes.csv"), uid) is None

    def test_a_person_still_works_in_their_workspace_and_the_flow_folders(self, alice, home):
        own = _workspace(alice)
        _new_flow(alice, own / "churn.yaml")
        assert alice.get("/files/directory_contents/", params={"directory": str(own)}).status_code == 200
        _new_flow(alice, storage.flows_directory / "catalog_flow.yaml")
        (storage.outputs_directory / "result.csv").write_text("a\n1\n")
        got = alice.get("/files/download", params={"path": str(storage.outputs_directory / "result.csv")})
        assert got.status_code == 200


class TestProjectFolders:
    def test_a_person_browses_their_own_project_folder_and_nobody_elses(self, alice, bob, users, tmp_path):
        mine = tmp_path / "projects" / str(users["bob"].id)
        theirs = tmp_path / "projects" / str(users["alice"].id)
        for folder in (mine, theirs):
            folder.mkdir(parents=True)
        assert bob.get("/files/directory_contents/", params={"directory": str(mine)}).status_code == 200
        made = bob.post("/files/create_directory", json={"source_path": str(mine), "dir_name": "my-project"})
        assert made.status_code == 200 and (mine / "my-project").is_dir()
        assert bob.get("/files/directory_contents/", params={"directory": str(theirs)}).status_code == 403
        refused = bob.post("/files/create_directory", json={"source_path": str(theirs), "dir_name": "planted"})
        assert refused.status_code == 403 and not (theirs / "planted").exists()
        assert (
            bob.get("/files/directory_contents/", params={"directory": str(tmp_path / "projects")}).status_code == 403
        )

    def test_a_project_folder_is_no_button_of_the_browser_and_no_place_of_a_file_node(self, bob, users, tmp_path):
        mine = tmp_path / "projects" / str(users["bob"].id)
        mine.mkdir(parents=True)
        assert [root["name"] for root in bob.get("/files/roots").json()] == ["Workspace", "Shared"]
        assert workspace.data_place(str(mine / "x.csv"), users["bob"].id) is None


class TestManyFiles:
    def _tree(self, root: Path) -> Path:
        big = root / "big"
        for d in range(30):
            (big / f"d{d}").mkdir(parents=True)
            for f in range(100):
                (big / f"d{d}" / f"f{f}.csv").write_text("a\n1\n")
        return big

    def test_a_listing_and_a_pattern_over_many_files_cost_little_more_than_the_walk(self, users, tmp_path):
        import glob
        import time

        from flowfile_core.fileExplorer.funcs import SecureFileExplorer

        big = self._tree(tmp_path)
        uid = users["bob"].id

        def timed(work) -> float:
            started = time.perf_counter()
            work()
            return time.perf_counter() - started

        plain = min(
            timed(lambda: SecureFileExplorer(str(big), str(tmp_path)).list_contents(recursive=True)) for _ in range(3)
        )
        listed = min(
            timed(lambda: workspace.explorer_for(str(big), uid, None).list_contents(recursive=True)) for _ in range(3)
        )
        pattern = str(big / "**" / "*.csv")
        walk = min(timed(lambda: glob.glob(pattern, recursive=True, include_hidden=True)) for _ in range(3))
        checked = min(timed(lambda: workspace.require_data_paths(uid, pattern, expand_glob=True)) for _ in range(3))
        print(
            f"\n3,000 files: list {listed:.3f}s (plain {plain:.3f}s), pattern check {checked:.3f}s (glob {walk:.3f}s)"
        )
        assert len(workspace.explorer_for(str(big), uid, None).list_contents(recursive=True)) == 3030
        assert listed < plain * 2 + 0.1
        assert checked < walk + 0.5


class TestAPipInstall:
    def test_deleting_an_account_leaves_the_folders_of_the_home_alone(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FLOWFILE_MODE", "package")
        mine = tmp_path / "workspaces" / "7"
        mine.mkdir(parents=True)
        (mine / "notes.txt").write_text("a folder of the person's own")
        assert workspace.retire_workspace(7) is None
        assert (mine / "notes.txt").exists()
        assert not (tmp_path / "workspaces" / workspace.DELETED_FOLDER).exists()


class TestFlowFiles:
    def test_a_person_saves_and_opens_a_flow_in_their_own_folder(self, alice):
        path = _workspace(alice) / "churn.yaml"
        flow_id = _new_flow(alice, path)
        assert path.exists()
        assert alice.post("/save_flow", params={"flow_id": flow_id, "register_in_catalog": False}).status_code == 200
        assert alice.get("/import_flow/", params={"flow_path": str(path)}).status_code == 200

    def test_another_persons_flow_is_not_opened(self, alice, bob):
        path = _workspace(alice) / "churn.yaml"
        _new_flow(alice, path)
        mine = _workspace(bob)
        (mine / "alias.yaml").symlink_to(path)
        for spelling in (str(path), str(mine / ".." / path.parent.name / path.name), str(mine / "alias.yaml")):
            response = bob.get("/import_flow/", params={"flow_path": spelling})
            assert response.status_code == 403, (spelling, response.text)
            assert REFUSED in response.json()["detail"] or "path traversal" in response.json()["detail"]

    def test_another_persons_flow_is_not_overwritten(self, alice, bob):
        path = _workspace(alice) / "churn.yaml"
        _new_flow(alice, path)
        before = path.read_text()
        flow_id = _new_flow(bob, _workspace(bob) / "mine.yaml")
        saved = bob.post(
            "/save_flow", params={"flow_id": flow_id, "flow_path": str(path), "register_in_catalog": False}
        )
        assert saved.status_code == 403
        created = bob.post("/editor/create_flow/", params={"flow_path": str(path), "register_in_catalog": False})
        assert created.status_code == 403
        assert path.read_text() == before

    def test_the_shared_and_flows_folders_still_take_a_flow(self, alice, tmp_path):
        _new_flow(alice, tmp_path / "team_flow.yaml")
        storage.flows_directory.mkdir(parents=True, exist_ok=True)
        _new_flow(alice, storage.flows_directory / "catalog_flow.yaml")

    def test_the_storage_and_working_folders_are_refused(self, alice):
        for folder in (storage.base_directory, Path(os.getcwd())):
            response = alice.post(
                "/editor/create_flow/",
                params={"flow_path": str(folder / "planted.yaml"), "register_in_catalog": False},
            )
            try:
                assert response.status_code == 403, (folder, response.text)
                assert not (folder / "planted.yaml").exists()
            finally:
                (folder / "planted.yaml").unlink(missing_ok=True)


class TestDesktopUnchanged:
    def test_a_desktop_has_no_workspace(self, monkeypatch, tmp_path):
        monkeypatch.setenv("FLOWFILE_MODE", "electron")
        assert workspace.browse_roots(1) == []
        assert workspace.data_place("/anything/at/all.csv", 1) == os.path.realpath("/anything/at/all.csv")


class TestCatalogTableFromAFile:
    def test_a_table_is_not_made_from_another_persons_file(self, alice, bob):
        private = _alice_file(alice)
        refused = bob.post("/catalog/tables", json={"name": "taken", "file_path": str(private)})
        assert refused.status_code == 403, refused.text
        assert REFUSED in refused.json()["detail"]


class TestUploadAndDownload:
    def _upload(self, client, folder: Path, name: str = "customers.csv", body: bytes = b"id,amount\n1,10\n2,20\n"):
        return client.post(
            "/file_manager/upload", params={"directory": str(folder)}, files={"file": (name, body, "text/csv")}
        )

    def test_a_csv_dropped_in_a_folder_is_read_by_a_flow_beside_it_and_its_result_downloaded(self, bob, users):
        import polars as pl

        from flowfile_core.flowfile.execution import registry
        from tests.sharing.test_workspace_nodes import _graph

        folder = _workspace(bob) / "churn"
        assert (
            bob.post(
                "/files/create_directory", json={"source_path": str(folder.parent), "dir_name": "churn"}
            ).status_code
            == 200
        )
        uploaded = self._upload(bob, folder)
        assert uploaded.status_code == 200, uploaded.text
        assert Path(uploaded.json()["filepath"]) == folder / "customers.csv"

        from flowfile_core.flowfile.flow_graph import add_connection
        from flowfile_core.schemas import input_schema

        graph = _graph()
        graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="read"))
        received = input_schema.ReceivedTable.create_from_path(str(folder / "customers.csv"), file_type="csv")
        graph.add_read(input_schema.NodeRead(flow_id=1, node_id=1, received_file=received, user_id=users["bob"].id))
        graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=2, node_type="output"))
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
        output = input_schema.OutputSettings(
            name="result.csv", directory=str(folder), file_type="csv", table_settings=input_schema.OutputCsvTable()
        )
        graph.add_output(input_schema.NodeOutput(flow_id=1, node_id=2, output_settings=output, user_id=users["bob"].id))
        with registry.acting_for(users["bob"].id):
            run_info = graph.run_graph()
        assert run_info.success, [r.error for r in run_info.node_step_result]

        downloaded = bob.get("/files/download", params={"path": str(folder / "result.csv")})
        assert downloaded.status_code == 200, downloaded.text
        assert pl.read_csv(downloaded.content)["amount"].to_list() == [10, 20]
        assert 'filename="result.csv"' in downloaded.headers["content-disposition"]

    def test_a_file_of_any_type_is_uploaded_into_a_folder(self, bob):
        mine = _workspace(bob)
        uploaded = self._upload(bob, mine, name="chart.png", body=b"\x89PNG\r\n")
        assert uploaded.status_code == 200, uploaded.text
        assert (mine / "chart.png").read_bytes() == b"\x89PNG\r\n"

    def test_a_yaml_file_is_uploaded_into_a_folder(self, bob):
        mine = _workspace(bob)
        uploaded = self._upload(bob, mine, name="Regional Sales Summary performance.yaml", body=b"a: 1\n")
        assert uploaded.status_code == 200, uploaded.text
        assert (mine / "Regional Sales Summary performance.yaml").read_bytes() == b"a: 1\n"

    def test_a_file_without_an_extension_is_uploaded_into_a_folder(self, bob):
        mine = _workspace(bob)
        uploaded = self._upload(bob, mine, name="README", body=b"notes\n")
        assert uploaded.status_code == 200, uploaded.text
        assert (mine / "README").read_bytes() == b"notes\n"

    def test_an_uploaded_file_of_any_type_is_listed_in_its_folder(self, bob):
        mine = _workspace(bob)
        names = ["chart.png", "summary.yaml", "customers.csv", "README"]
        for name in names:
            assert self._upload(bob, mine, name=name, body=b"x\n").status_code == 200, name
        listed = bob.get("/files/directory_contents/", params={"directory": str(mine), "include_hidden": False})
        assert listed.status_code == 200, listed.text
        assert sorted(f["name"] for f in listed.json()) == sorted(names)

    def test_the_uploads_folder_takes_data_files_only(self, bob):
        refused = bob.post("/file_manager/upload", files={"file": ("chart.png", b"\x89PNG\r\n", "image/png")})
        assert refused.status_code == 400
        assert "not allowed" in refused.json()["detail"]

    def test_nothing_is_uploaded_into_another_persons_folder(self, alice, bob):
        theirs = _workspace(alice)
        refused = self._upload(bob, theirs, name="planted.csv")
        assert refused.status_code == 403
        assert not (theirs / "planted.csv").exists()

    def test_another_persons_file_is_not_downloaded(self, alice, bob):
        private = _alice_file(alice)
        mine = _workspace(bob)
        (mine / "peek.csv").symlink_to(private)
        for path in (private, mine / ".." / private.parent.name / private.name, mine / "peek.csv"):
            assert bob.get("/files/download", params={"path": str(path)}).status_code == 403, path

    def test_a_folder_is_not_downloaded(self, bob):
        assert bob.get("/files/download", params={"path": str(_workspace(bob))}).status_code == 400
