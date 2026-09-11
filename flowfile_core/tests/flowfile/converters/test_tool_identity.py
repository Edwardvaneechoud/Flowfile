"""The canonical tool key: the one place that decides which Alteryx names may leave the machine."""

import pytest

from flowfile_core.flowfile.converters.alteryx import tool_identity


@pytest.mark.parametrize(
    ("plugin", "expected"),
    [
        ("AlteryxBasePluginsGui.Filter.Filter", "Filter"),
        ("AlteryxGuiToolkit.TextBox.TextBox", "TextBox"),
        ("AlteryxSpatialPluginsGui.RunningTotal.RunningTotal", "RunningTotal"),
        ("AlteryxConnectorGui.Download.Download", "Download"),
        ("Cleanse.yxmc", "macro_cleanse"),
        ("C:\\Program Files\\Alteryx\\bin\\RuntimeData\\Macros\\CountRecords.yxmc", "macro_countrecords"),
        ("cleanse.YXMC", "macro_cleanse"),
        ("C:\\Users\\x\\Quarterly Revenue (confidential).yxmc", "user_macro"),
        ("/Users/x/secret.csv.yxmc", "user_macro"),
        ("AcmeCorp.SecretUploader.SecretUploader", "custom_plugin"),
        ("AlteryxBasePluginsGui", "custom_plugin"),
        ("AlteryxBasePluginsGui.Bad Name.Bad Name", "custom_plugin"),
        ("", "custom_plugin"),
    ],
)
def test_tool_key(plugin: str, expected: str) -> None:
    assert tool_identity.tool_key(plugin) == expected


@pytest.mark.parametrize(
    ("key", "expected"),
    [("Filter", True), ("macro_cleanse", True), ("custom_plugin", False), ("user_macro", False)],
)
def test_is_official(key: str, expected: bool) -> None:
    assert tool_identity.is_official(key) is expected


def test_every_key_is_a_telemetry_safe_identifier() -> None:
    for plugin in ["AlteryxBasePluginsGui.Filter.Filter", "Cleanse.yxmc", "Acme.X.Y", "x.yxmc", ""]:
        key = tool_identity.tool_key(plugin)
        assert key.isidentifier() and len(key) <= tool_identity.MAX_KEY_LENGTH
