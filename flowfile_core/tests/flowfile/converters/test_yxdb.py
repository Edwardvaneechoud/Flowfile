"""Tests for the Alteryx .yxdb -> Parquet converter.

No `.yxdb` fixture is checked in: the format has no writer outside Alteryx, and Alteryx's own
sample files must not be redistributed. These tests therefore run against a private
corpus in `alteryx_nodes_data/` (a sibling of the repo) and skip cleanly when it is absent.

Four files were picked for dtype coverage (a fifth, `Presidents_and_VPs.yxdb`, is the only
small file with real nulls). Between them they exercise 13 of the 16 Alteryx field types;
`Bool`, `Blob` and `WString` appear nowhere in the 96-file corpus, so those three table rows
stay unproven by data.

The AMP-engine (E2) half of the module is a different reader (`sigilyx`, the `alteryx-amp`
extra) and a guard, and has its own section at the end of this file. It is exercised against
the corpus' three E2 files, plus one synthetic frame for the guard itself.
"""

from __future__ import annotations

import importlib.util
import xml.etree.ElementTree as ET
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from flowfile_core.flowfile.converters.alteryx.yxdb import (
    E1_MAGIC,
    E2_MAGIC,
    _all_null_rows,
    _container,
    _encoding_warnings,
    _refuse_dropped_records,
    convert_tree,
    convert_yxdb,
    read_yxdb,
)

DATA_DIR = Path(__file__).resolve().parents[5] / "alteryx_nodes_data"
CORPUS_DIR = Path(__file__).resolve().parents[5] / "alteryx_nodes"

pytest.importorskip("yxdb", reason="optional extra: pip install 'flowfile[yxdb]'")
pytestmark = pytest.mark.skipif(not DATA_DIR.is_dir(), reason=f"Alteryx sample data not present at {DATA_DIR}")

# (file, rows Alteryx recorded in the .yxdb header)
CUSTOMER_FILE_4 = ("OneToolData/CustomerFile4.yxdb", 90)
REPORT_MAPPING_1 = ("OneToolData/ReportMapping1.yxdb", 5)
TUTORIAL_DATA = ("SampleData/TutorialData.yxdb", 8716)
COUNTY_DENSITY = ("SampleData/CountyDensity.yxdb", 3141)

# The dtype table of yxdb.py, asserted against real files: (file, column, Alteryx type, Polars dtype)
DTYPE_CASES = [
    (CUSTOMER_FILE_4, "CustomerID", "Int32", pl.Int32),
    (CUSTOMER_FILE_4, "FirstName", "String", pl.String),
    (CUSTOMER_FILE_4, "LastName", "V_String", pl.String),
    (CUSTOMER_FILE_4, "Visits", "Byte", pl.Int32),
    (CUSTOMER_FILE_4, "Spend", "Double", pl.Float64),
    (CUSTOMER_FILE_4, "SpatialObj__spatial", "SpatialObj", pl.Binary),
    (CUSTOMER_FILE_4, "JoinDate", "Date", pl.Date),
    (REPORT_MAPPING_1, "LATITUDE", "Float", pl.Float32),
    (TUTORIAL_DATA, "First", "V_WString", pl.String),
    (TUTORIAL_DATA, "Birth Date", "DateTime", pl.Datetime),
    (COUNTY_DENSITY, "DEN90", "Int16", pl.Int32),
    (COUNTY_DENSITY, "POP1990", "Int32", pl.Int32),
]


def frame_of(case: tuple[str, int]) -> pl.DataFrame:
    frame, _ = read_yxdb(DATA_DIR / case[0])
    return frame


@pytest.fixture(scope="module")
def frames() -> dict[str, pl.DataFrame]:
    """Read each test file once; TutorialData is 8716 rows and every case re-reads otherwise."""
    return {case[0]: frame_of(case) for case in (CUSTOMER_FILE_4, REPORT_MAPPING_1, TUTORIAL_DATA, COUNTY_DENSITY)}


@pytest.mark.parametrize("case, rows", [CUSTOMER_FILE_4, REPORT_MAPPING_1, TUTORIAL_DATA, COUNTY_DENSITY])
def test_row_count_matches_the_header_alteryx_wrote(frames, case: str, rows: int):
    assert frames[case].height == rows


@pytest.mark.parametrize("case, column, alteryx_type, dtype", DTYPE_CASES)
def test_dtype_table(frames, case, column, alteryx_type, dtype):
    assert frames[case[0]].schema[column] == dtype, f"{alteryx_type} should map to {dtype}"


def test_datetime_is_microsecond_precision(frames):
    assert frames[TUTORIAL_DATA[0]].schema["Birth Date"] == pl.Datetime("us")


def test_date_stays_a_date_not_a_datetime(frames):
    """Alteryx Date has no time part and yxdb hands back a datetime; it must not widen."""
    column = frames[CUSTOMER_FILE_4[0]]["JoinDate"]
    assert column.dtype == pl.Date
    assert column[0].isoformat() == "2013-08-06"


def test_fixed_decimal_keeps_its_declared_scale(frames):
    column = frames[COUNTY_DENSITY[0]]["AreaSqMi"]
    assert column.dtype == pl.Decimal(11, 2)
    value = column[0]
    assert isinstance(value, Decimal)
    assert value.as_tuple().exponent == -2


def test_non_ascii_wide_string_survives(frames):
    """V_WString is utf-16-le on disk; these two rows are the corpus' only non-ASCII names."""
    last = frames[TUTORIAL_DATA[0]]["Last"]
    assert last[31] == "Karaböcek"
    assert last[33] == "Ilıcalı"


def test_spatial_column_is_renamed_and_warns():
    frame, warnings = read_yxdb(DATA_DIR / CUSTOMER_FILE_4[0])
    assert "SpatialObj" not in frame.columns
    assert frame.schema["SpatialObj__spatial"] == pl.Binary
    assert any("spatial" in warning for warning in warnings)


def test_nulls_stay_null_without_widening_the_dtype():
    """44 of the 91 presidents have no vice-president number; the column must stay Int32."""
    frame, _ = read_yxdb(DATA_DIR / "OneToolData" / "Presidents_and_VPs.yxdb")

    assert frame.schema["VicePresidentNo"] == pl.Int32
    assert frame["VicePresidentNo"].null_count() == 44


def test_schema_matches_the_recordinfo_alteryx_cached_in_the_workflow():
    """Independent source of truth: Summarize.yxmd carries Alteryx's own field list for this file."""
    if not CORPUS_DIR.is_dir():
        pytest.skip("Alteryx .yxmd corpus not present")
    workflow = CORPUS_DIR / "05 Transform" / "Summarize.yxmd"
    record_info = ET.fromstring(workflow.read_bytes()).iter("RecordInfo")
    cached = [(f.get("name"), f.get("type")) for f in next(record_info).iter("Field")]
    expected = {
        "Int32": pl.Int32,
        "Byte": pl.Int32,
        "Double": pl.Float64,
        "String": pl.String,
        "V_String": pl.String,
        "Date": pl.Date,
        "SpatialObj": pl.Binary,
    }
    schema = frame_of(CUSTOMER_FILE_4).schema
    assert len(cached) == len(schema)
    for (name, alteryx_type), (built, dtype) in zip(cached, schema.items(), strict=True):
        assert built == (f"{name}__spatial" if alteryx_type == "SpatialObj" else name)
        assert dtype == expected[alteryx_type]


def test_convert_writes_a_parquet_sibling_by_default(tmp_path: Path):
    source = tmp_path / "CustomerFile4.yxdb"
    source.write_bytes((DATA_DIR / CUSTOMER_FILE_4[0]).read_bytes())

    stats = convert_yxdb(source)

    assert stats.destination == tmp_path / "CustomerFile4.parquet"
    assert stats.rows == 90
    assert stats.columns == 11
    assert stats.error is None
    assert pl.read_parquet(stats.destination).height == 90


def test_convert_skips_an_existing_destination_unless_overwrite(tmp_path: Path):
    destination = tmp_path / "out.parquet"
    destination.write_bytes(b"not parquet")

    skipped = convert_yxdb(DATA_DIR / REPORT_MAPPING_1[0], destination)
    assert skipped.skipped is True
    assert destination.read_bytes() == b"not parquet"

    written = convert_yxdb(DATA_DIR / REPORT_MAPPING_1[0], destination, overwrite=True)
    assert written.skipped is False
    assert pl.read_parquet(destination).height == 5


def test_csv_output_hex_encodes_binary_columns(tmp_path: Path):
    stats = convert_yxdb(DATA_DIR / REPORT_MAPPING_1[0], tmp_path / "out.csv", csv=True)

    assert stats.rows == 5
    assert pl.read_csv(stats.destination).schema["CENTROID__spatial"] == pl.String
    assert any("hex-encoded" in warning for warning in stats.warnings)


def test_convert_tree_mirrors_the_source_layout(tmp_path: Path):
    source = tmp_path / "src" / "nested"
    source.mkdir(parents=True)
    (source / "a.yxdb").write_bytes((DATA_DIR / REPORT_MAPPING_1[0]).read_bytes())

    results = convert_tree(tmp_path / "src", tmp_path / "out")

    assert len(results) == 1
    assert results[0].destination == tmp_path / "out" / "nested" / "a.parquet"
    assert results[0].destination.exists()


def test_convert_tree_records_a_failure_instead_of_raising(tmp_path: Path):
    (tmp_path / "broken.yxdb").write_bytes(b"not an Alteryx file at all" + b"\0" * 512)
    (tmp_path / "good.yxdb").write_bytes((DATA_DIR / REPORT_MAPPING_1[0]).read_bytes())

    results = sorted(convert_tree(tmp_path), key=lambda s: s.source.name)

    assert results[0].error is not None
    assert "not a supported .yxdb" in results[0].error
    assert results[1].ok


def test_an_unsupported_field_type_is_named_in_the_error(tmp_path: Path):
    """The corpus' one Time column: yxdb 1.1.1 has no extractor, and the message must say so."""
    source = DATA_DIR / "OneToolData" / "NYC_Collisions_2015_Queens.yxdb"
    if not source.exists():
        pytest.skip("file not in the sample data")

    with pytest.raises(RuntimeError, match="Time"):
        convert_yxdb(source, tmp_path / "out.parquet")


# --- AMP engine (Alteryx e2) -----------------------------------------------------------------

# A module-level importorskip would take the E1 tests with it: the two readers are two extras.
needs_amp = pytest.mark.skipif(
    importlib.util.find_spec("sigilyx") is None, reason="optional extra: pip install 'flowfile[alteryx-amp]'"
)

# The three AMP files in the corpus; all three are spatial. (file, rows, columns)
E2_FILES = [
    ("SampleData/AdArea polygons.yxdb", 11, 3),
    ("SampleData/Wyoming ZIPs.yxdb", 140, 5),
    ("SampleData/Sample Streets.yxdb", 3598, 2),
]


@pytest.mark.parametrize(
    "header, container",
    [
        (E1_MAGIC + b"\x00" * 8, "e1"),
        (E2_MAGIC + b"\x00" * 8, "e2"),
        (b"Alteryx Database", None),
        (b"PAR1" + b"\x00" * 8, None),
        (b"", None),
    ],
)
def test_the_container_is_read_from_the_magic_string_alone(tmp_path: Path, header: bytes, container: str | None):
    """Which reader a file gets is decided here and nowhere else."""
    source = tmp_path / "header_only.yxdb"
    source.write_bytes(header)
    assert _container(source) == container


@pytest.mark.parametrize("case, rows, columns", E2_FILES)
@needs_amp
def test_an_amp_engine_file_converts(tmp_path: Path, case: str, rows: int, columns: int):
    """`yxdb` cannot open these at all; sigilyx can, and all three survive the guard."""
    stats = convert_yxdb(DATA_DIR / case, tmp_path / "out.parquet")

    assert stats.error is None
    assert (stats.rows, stats.columns) == (rows, columns)
    written = pl.read_parquet(stats.destination)
    assert written.height == rows
    assert written.schema["SpatialObj__spatial"] == pl.Binary
    assert not any("refused" in warning for warning in stats.warnings)


@pytest.mark.parametrize("case, rows, columns", E2_FILES)
@needs_amp
def test_no_amp_corpus_file_comes_back_with_a_blank_record(case: str, rows: int, columns: int):
    """The guard's input, measured per file: every record decodes, so 0 rows are fully null."""
    frame, _ = read_yxdb(DATA_DIR / case)

    blank = frame.select(pl.all_horizontal(pl.all().is_null()).alias("blank"))["blank"].sum()
    assert (frame.height, blank) == (rows, 0)


def test_the_guard_refuses_a_file_whose_records_came_back_blank():
    """Unit test of the guard on a synthetic tally: this is the shape Edward's real file returns."""
    with pytest.raises(RuntimeError) as failure:
        _refuse_dropped_records({"rows": 10_000, "columns": 24, "all_null_rows": 9_918})

    message = str(failure.value)
    assert "10000 record(s)" in message and "9918 are entirely null" in message
    assert "experimental" in message and "nothing was written" in message

    _refuse_dropped_records({"rows": 10_000, "columns": 24, "all_null_rows": 0})


def test_a_narrow_string_column_that_lost_bytes_is_named_on_the_warnings():
    """sigilyx reads Alteryx's single-byte String fields as UTF-8; no corpus AMP file has one."""
    assert _encoding_warnings({"rows": 3, "columns": 2, "all_null_rows": 0}) == []

    lost = _encoding_warnings({"rows": 3, "columns": 2, "all_null_rows": 0, "City": 66})[0]
    assert "'City' (66 value(s))" in lost and "U+FFFD" in lost


def test_the_guard_counts_fully_null_rows_and_not_merely_null_cells():
    """A row with one null must not trip the guard; a row that is all null must."""
    frame = pl.DataFrame({"a": [1, None, None], "b": ["x", "y", None], "c": [None, None, None]})

    assert _all_null_rows(frame) == 1
    assert _all_null_rows(frame.head(0)) == 0


@needs_amp
def test_a_refused_amp_file_writes_no_parquet(tmp_path: Path, monkeypatch):
    """The refusal must leave nothing behind — not the output, not the .part file."""
    monkeypatch.setattr(
        "flowfile_core.flowfile.converters.alteryx.yxdb._all_null_rows", lambda batch: batch.height
    )
    destination = tmp_path / "out.parquet"

    with pytest.raises(RuntimeError, match="entirely null"):
        convert_yxdb(DATA_DIR / E2_FILES[0][0], destination)

    assert not destination.exists()
    assert list(tmp_path.iterdir()) == []


@needs_amp
def test_an_amp_engine_file_says_its_spatial_decoder_is_experimental(tmp_path: Path):
    """sigilyx has never verified its E2 SpatialObj/Time/WString/Blob decoders against real files."""
    stats = convert_yxdb(DATA_DIR / E2_FILES[1][0], tmp_path / "out.parquet")

    experimental = next(warning for warning in stats.warnings if "experimental" in warning)
    assert "AMP engine" in experimental
    assert "SpatialObj column(s) ('SpatialObj')" in experimental


@needs_amp
def test_an_amp_engine_files_non_spatial_columns_round_trip():
    """Alteryx's own cached RecordInfo for `Sample Streets.yxdb` is in the Spatial_Info workflow."""
    if not CORPUS_DIR.is_dir():
        pytest.skip("Alteryx .yxmd corpus not present")
    workflow = CORPUS_DIR / "09 Spatial" / "Spatial_Info.yxmd"
    cached = [
        [(f.get("name"), f.get("type")) for f in record_info.iter("Field")]
        for record_info in ET.fromstring(workflow.read_bytes()).iter("RecordInfo")
    ]
    assert [("NAME", "V_String"), ("SpatialObj", "SpatialObj")] in cached

    frame, _ = read_yxdb(DATA_DIR / "SampleData" / "Sample Streets.yxdb")

    assert frame.columns == ["NAME", "SpatialObj__spatial"]
    assert frame["NAME"][:3].to_list() == ["Unnamed Street", "S Pine St", "S 2nd St"]


@needs_amp
def test_an_e2_file_that_cannot_be_decoded_is_refused_quoting_the_reader(tmp_path: Path):
    """An E2 magic string is not a promise the body decodes; the refusal says which is which."""
    source = tmp_path / "liquor_sales.yxdb"
    source.write_bytes(E2_MAGIC + b"\x00" * 600)

    results = convert_tree(tmp_path)

    assert results[0].error is not None
    assert "AMP-engine (E2)" in results[0].error
    assert "E2 file ID" in results[0].error, "sigilyx's own complaint must survive into the message"


@needs_amp
def test_an_amp_conversion_streams_instead_of_holding_the_whole_file(tmp_path: Path, monkeypatch):
    """A 1.4 GB AMP file must not become one frame: the writer pulls batches from the reader."""
    from flowfile_core.flowfile.converters.alteryx.yxdb import BATCH_ROWS

    assert BATCH_ROWS == 65_536
    monkeypatch.setattr("flowfile_core.flowfile.converters.alteryx.yxdb.BATCH_ROWS", 500)
    seen: list[int] = []
    import sigilyx

    real_batches = sigilyx.read_yxdb_batches

    def counting_batches(*args, **kwargs):
        for batch in real_batches(*args, **kwargs):
            seen.append(batch.height)
            yield batch

    monkeypatch.setattr(sigilyx, "read_yxdb_batches", counting_batches)

    stats = convert_yxdb(DATA_DIR / E2_FILES[2][0], tmp_path / "out.parquet")

    assert stats.rows == 3598
    assert len(seen) == 8 and max(seen) == 500, "the reader was asked for batches, not for the file"
    assert pl.read_parquet(stats.destination).height == 3598
