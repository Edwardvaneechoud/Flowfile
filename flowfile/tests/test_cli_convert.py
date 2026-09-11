"""Tests for CLI 'flowfile convert yxdb'.

Run with:
    pytest flowfile/tests/test_cli_convert.py -v

Needs Edward's private Alteryx sample corpus next to the repo; skips cleanly without it.
"""

from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from flowfile.__main__ import main

DATA_DIR = Path(__file__).resolve().parents[3] / "alteryx_nodes_data"
SAMPLE = DATA_DIR / "OneToolData" / "ReportMapping1.yxdb"

pytest.importorskip("yxdb", reason="optional extra: pip install 'flowfile[alteryx]'")
pytestmark = pytest.mark.skipif(not SAMPLE.exists(), reason=f"Alteryx sample data not present at {DATA_DIR}")


def run_cli(*argv: str) -> int:
    with patch('sys.argv', ['flowfile', *argv]), pytest.raises(SystemExit) as exit_info:
        main()
    return exit_info.value.code


def test_convert_one_file_into_an_out_dir(tmp_path: Path, capsys):
    assert run_cli('convert', 'yxdb', str(SAMPLE), '--out', str(tmp_path)) == 0

    written = tmp_path / 'ReportMapping1.parquet'
    assert pl.read_parquet(written).height == 5

    captured = capsys.readouterr()
    assert '5 rows x 9 cols' in captured.out
    assert '1 converted, 0 skipped, 0 failed' in captured.out


def test_convert_a_directory_recursively(tmp_path: Path, capsys):
    nested = tmp_path / 'src' / 'nested'
    nested.mkdir(parents=True)
    (nested / 'copy.yxdb').write_bytes(SAMPLE.read_bytes())

    assert run_cli('convert', 'yxdb', str(tmp_path / 'src'), '--out', str(tmp_path / 'out')) == 0
    assert (tmp_path / 'out' / 'nested' / 'copy.parquet').exists()

    assert run_cli('convert', 'yxdb', str(tmp_path / 'src'), '--out', str(tmp_path / 'out')) == 0
    assert '0 converted, 1 skipped, 0 failed' in capsys.readouterr().out


def test_csv_flag_writes_csv(tmp_path: Path):
    assert run_cli('convert', 'yxdb', str(SAMPLE), '--out', str(tmp_path), '--csv') == 0
    assert (tmp_path / 'ReportMapping1.csv').exists()


def test_an_unreadable_file_fails_the_command_but_not_the_walk(tmp_path: Path, capsys):
    (tmp_path / 'broken.yxdb').write_bytes(b'nonsense' + b'\0' * 600)
    (tmp_path / 'good.yxdb').write_bytes(SAMPLE.read_bytes())

    assert run_cli('convert', 'yxdb', str(tmp_path), '--out', str(tmp_path / 'out')) == 1

    captured = capsys.readouterr()
    assert 'FAILED' in captured.err
    assert '1 converted, 0 skipped, 1 failed' in captured.out
    assert (tmp_path / 'out' / 'good.parquet').exists()


def test_missing_path_reports_usage(capsys):
    assert run_cli('convert', 'yxdb') == 1
    assert 'Usage: flowfile convert yxdb' in capsys.readouterr().err


def test_convert_appears_in_help(capsys):
    with patch('sys.argv', ['flowfile']):
        main()
    assert 'flowfile convert yxdb' in capsys.readouterr().out
