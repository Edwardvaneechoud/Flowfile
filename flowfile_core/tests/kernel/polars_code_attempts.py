"""The attempts of GHSA-f34x-gw22-9wwh as Polars Code, for tests that prove none of them runs in a server process.

Each is code the sandbox accepts on a desktop. ``attempts(work)`` aims them at scratch files under ``work`` and at a
port nothing listens on, so a test that let one run would touch nothing real.
"""

import sqlite3
from pathlib import Path

NAMES = (
    "file_read",
    "file_write",
    "environment_by_format_string",
    "catalog_sqlite_read",
    "request_from_the_servers_network",
    "sql_table_function",
    "os_by_a_generators_frame",
    "proc_self_environ",
    "file_write_through_config",
)


def written_files(work: Path) -> list[Path]:
    """The files the write attempts would leave behind."""
    return [work / "written_by_polars_code.txt", work / "written_by_config.json"]


def attempts(work: Path) -> dict[str, str]:
    """Each attempt's code by name, with the file and the catalog its read attempts go for in place."""
    secret = work / "other_users_file.txt"
    secret.write_text("FAKE-FILE-CONTENT-of-another-user\n")
    catalog = work / "fake_catalog.db"
    if not catalog.exists():
        with sqlite3.connect(catalog) as connection:
            connection.execute("create table users (id integer, username text, is_admin integer)")
            connection.execute("insert into users values (1, 'fake_admin', 1)")
    written, config = written_files(work)
    codes = {
        "file_read": f"output_df = pl.scan_csv({str(secret)!r}, has_header=False, separator='\\x01')",
        "file_write": (
            f"pl.DataFrame({{'c': ['any text']}}).write_csv({str(written)!r}, include_header=False, "
            "quote_style='never')\noutput_df = input_df"
        ),
        "environment_by_format_string": (
            "u = '_' * 2\n"
            "template = '{0.' + u + 'globals' + u + '[os].environ[FLOWFILE_MASTER_KEY]}'\n"
            "output_df = pl.LazyFrame({'leak': [template.format(pl.business_day_count)]})"
        ),
        "catalog_sqlite_read": (
            f"output_df = pl.read_database_uri('select * from users', 'sqlite://{catalog}').lazy()"
        ),
        "request_from_the_servers_network": "output_df = pl.read_csv('http://127.0.0.1:9/served.csv').lazy()",
        "sql_table_function": f"output_df = pl.SQLContext().execute(\"select * from read_csv('{secret}')\")",
        "os_by_a_generators_frame": (
            "def walk():\n"
            "    yield probe.gi_frame.f_back.f_back.f_globals\n"
            "probe = walk()\n"
            "found = [scope for scope in probe][0]\n"
            "output_df = pl.LazyFrame({'pid': [found['os'].getpid()], "
            "'key': [found['os'].environ['FLOWFILE_MASTER_KEY']]})"
        ),
        "proc_self_environ": (
            "output_df = pl.read_csv('/proc/self/environ', has_header=False, separator='\\x01', quote_char=None, "
            "eol_char='\\x00', infer_schema=False).lazy()"
        ),
        "file_write_through_config": f"pl.Config().save_to_file({str(config)!r})\noutput_df = input_df",
    }
    assert tuple(codes) == NAMES
    return codes
