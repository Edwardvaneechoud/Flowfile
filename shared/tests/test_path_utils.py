import glob
import os

import pytest

from shared.path_utils import (
    DIRECTORY_SCAN_FILE_TYPES,
    DirectoryScanUnsupportedError,
    NoFilesMatchedError,
    assert_directory_scan_supported,
    default_scan_extension,
    ensure_glob_pattern,
    expand_glob_pattern,
    is_glob_pattern,
    is_url,
)


def test_is_url_detects_http_and_https():
    assert is_url("http://example.com/data.csv")
    assert is_url("https://raw.githubusercontent.com/org/repo/main/data.csv")


def test_is_url_rejects_local_paths():
    assert not is_url("/app/data/regions.csv")
    assert not is_url("data/regions.csv")
    assert not is_url("~/Downloads/regions.csv")
    assert not is_url("C:\\Users\\me\\regions.csv")


def test_is_url_handles_non_string():
    assert not is_url(None)


def test_is_glob_pattern_detects_metacharacters():
    """Only *, ? and [ make a path a pattern; a plain path must stay a plain path."""
    assert not is_glob_pattern("/data/sales/january.csv")
    assert is_glob_pattern("/data/sales/*.csv")
    assert is_glob_pattern("/data/sales/part_?.csv")
    assert is_glob_pattern("/data/sales/[ab].csv")


def test_is_glob_pattern_ignores_parameter_references():
    """${param} carries braces but no glob meaning — the substituted value decides globness."""
    assert not is_glob_pattern("${data_dir}")
    assert not is_glob_pattern("/data/${env}/sales")
    assert is_glob_pattern("${data_dir}/*.csv")


def test_default_scan_extension_maps_ipc_to_arrow():
    """The synthesised glob must use the on-disk extension, which differs from the file type for ipc."""
    assert default_scan_extension("ipc") == "arrow"
    assert default_scan_extension("csv") == "csv"
    assert default_scan_extension("parquet") == "parquet"


def test_ensure_glob_pattern_synthesises_recursive_glob_for_a_directory(tmp_path):
    """A bare directory becomes a recursive glob so nested files are read too."""
    assert ensure_glob_pattern(str(tmp_path), "csv") == os.path.join(str(tmp_path), "**", "*.csv")


def test_ensure_glob_pattern_leaves_an_explicit_pattern_alone(tmp_path):
    """A user-written pattern is authoritative — never wrapped in another /**/*.ext."""
    pattern = os.path.join(str(tmp_path), "part_*.csv")
    assert ensure_glob_pattern(pattern, "csv") == pattern


def test_ensure_glob_pattern_treats_a_real_bracketed_directory_as_a_directory(tmp_path):
    """What is on disk wins over a metacharacter sniff: a directory literally named ``[ab]`` is a
    directory, and its escaped pattern must not match the sibling file the character class would."""
    directory = tmp_path / "[ab]"
    directory.mkdir()
    (directory / "one.csv").write_text("a\n1\n")
    (directory / "two.csv").write_text("a\n2\n")
    (tmp_path / "a").write_text("decoy matched only if [ab] is read as a character class\n")

    pattern = ensure_glob_pattern(str(directory), "csv")

    assert pattern == os.path.join(glob.escape(str(directory)), "**", "*.csv")
    assert expand_glob_pattern(pattern) == [str(directory / "one.csv"), str(directory / "two.csv")]


def test_ensure_glob_pattern_synthesises_for_a_directory_that_does_not_exist_yet(tmp_path):
    """Nothing on disk and no metacharacters still means 'a directory' — it may appear before the run.

    An absent path cannot be escaped meaningfully, and matches nothing either way.
    """
    missing = tmp_path / "landing"

    assert ensure_glob_pattern(str(missing), "csv") == os.path.join(str(missing), "**", "*.csv")


def test_ensure_glob_pattern_passes_an_existing_file_through(tmp_path):
    """A directory-mode node pointed at one concrete file still reads that file."""
    file_path = tmp_path / "sales.csv"
    file_path.write_text("a\n1\n")
    assert ensure_glob_pattern(str(file_path), "csv") == str(file_path)


def test_expand_glob_pattern_is_sorted_and_files_only(tmp_path):
    """The expansion is the single source of truth for scan, fingerprint and probe, so it must be
    deterministic (sorted) and never hand a directory to polars."""
    (tmp_path / "b.csv").write_text("a\n1\n")
    (tmp_path / "a.csv").write_text("a\n2\n")
    (tmp_path / "looks_like_a_file.csv").mkdir()

    assert expand_glob_pattern(str(tmp_path / "*.csv")) == [str(tmp_path / "a.csv"), str(tmp_path / "b.csv")]


def test_expand_glob_pattern_excludes_dotfiles(tmp_path):
    """Python's glob skips dotfiles by design, which keeps macOS AppleDouble junk out of the scan."""
    (tmp_path / "data.csv").write_text("a\n1\n")
    (tmp_path / ".hidden.csv").write_text("a\n2\n")

    assert expand_glob_pattern(str(tmp_path / "*.csv")) == [str(tmp_path / "data.csv")]


@pytest.mark.parametrize("file_type", ["csv", "parquet", "ipc"])
def test_assert_directory_scan_supported_accepts_natively_scannable_types(file_type):
    """Only formats polars can scan from a file list may be read as a directory."""
    assert file_type in DIRECTORY_SCAN_FILE_TYPES
    assert_directory_scan_supported(file_type)


@pytest.mark.parametrize("file_type", ["excel", "avro", "json", "ndjson"])
def test_assert_directory_scan_supported_refuses_other_types(file_type):
    """Refusal happens up front so the user sees a clear error instead of a partial read."""
    assert file_type not in DIRECTORY_SCAN_FILE_TYPES
    with pytest.raises(DirectoryScanUnsupportedError):
        assert_directory_scan_supported(file_type)


def test_assert_directory_scan_supported_refuses_non_utf8_csv():
    """Non-UTF-8 csv routes to the worker's separate reader, which directory mode must never reach."""
    with pytest.raises(DirectoryScanUnsupportedError):
        assert_directory_scan_supported("csv", encoding="ISO-8859-1")


@pytest.mark.parametrize("encoding", ["utf-8", "utf8-lossy"])
def test_assert_directory_scan_supported_accepts_utf8_csv(encoding):
    """Both UTF-8 spellings the csv reader uses stay on the native scan path."""
    assert_directory_scan_supported("csv", encoding=encoding)


def test_assert_directory_scan_supported_refuses_urls():
    """There is no filesystem to expand a glob over, so directory mode over a URL is refused up front."""
    with pytest.raises(DirectoryScanUnsupportedError):
        assert_directory_scan_supported("csv", None, "https://example.com/data")
    with pytest.raises(DirectoryScanUnsupportedError):
        assert_directory_scan_supported("parquet", None, "http://example.com/data")


def test_assert_directory_scan_supported_accepts_a_local_path():
    """The path argument is optional and only ever refuses URLs — local paths stay on the happy path."""
    assert_directory_scan_supported("csv", "utf-8", "/data/sales")
    assert_directory_scan_supported("parquet", None, "/data/sales")


def test_no_files_matched_error_is_a_file_not_found_error():
    """Callers that already handle a missing file keep working when a pattern matches nothing."""
    assert issubclass(NoFilesMatchedError, FileNotFoundError)
