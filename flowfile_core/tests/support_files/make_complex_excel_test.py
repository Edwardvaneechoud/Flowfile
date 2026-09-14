"""Regenerate ``data/complex_excel_test.xlsx``, the deliberately awkward Read-node fixture.

Every sheet is one shape a real spreadsheet turns up in: title blocks above the table, stray cells
outside it, blank and duplicate headers, columns that change type partway down, Excel error values,
formulas with and without cached results. ``test_read_excel_tables.py`` pins what the reader does
with each one.

    poetry run python flowfile_core/tests/support_files/make_complex_excel_test.py
"""

import datetime
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill

OUT = Path(__file__).resolve().parent / "data" / "complex_excel_test.xlsx"

wb = Workbook()

# --- Sales Report: title block, table starting at row 5, subtotals, stray note in K1 ---
ws = wb.active
ws.title = "Sales Report"
ws["A1"] = "ACME Industries — Regional Sales"
ws["A1"].font = Font(bold=True, size=14)
ws.merge_cells("A1:E1")
ws["A2"] = "Generated 2026-03-14 by finance@acme.example"
ws["A3"] = None
ws["K1"] = "note: Q2 figures are provisional"  # stray cell, widens the used range
headers = ["Region", "Rep", "Q1", "Q2", "Q3", "Q4", "Total", "Margin %"]
for col, name in enumerate(headers, start=1):
    c = ws.cell(row=5, column=col, value=name)
    c.font = Font(bold=True)
    c.fill = PatternFill("solid", fgColor="DDDDDD")
rows = [
    ("North", "A. Okafor", 120000, 138500.5, 141000, 99000, None, 0.184),
    ("North", "B. Lindqvist", 88000, 91250.75, 87500, 93000, None, 0.211),
    ("North", "Subtotal", None, None, None, None, None, None),
    ("South", "C. Moreau", 143000, 150000, 162500.25, 171000, None, 0.157),
    ("South", "D. Yamamoto", 67000, 71000, 69500, 72000, None, 0.233),
    ("South", "Subtotal", None, None, None, None, None, None),
]
r = 6
for region, rep, q1, q2, q3, q4, _total, margin in rows:
    ws.cell(row=r, column=1, value=region)
    ws.cell(row=r, column=2, value=rep)
    for i, q in enumerate((q1, q2, q3, q4), start=3):
        ws.cell(row=r, column=i, value=q)
    if rep != "Subtotal":
        ws.cell(row=r, column=7, value=f"=SUM(C{r}:F{r})")
        ws.cell(row=r, column=8, value=margin).number_format = "0.0%"
    else:
        ws.cell(row=r, column=2).font = Font(bold=True, italic=True)
    r += 1
ws.cell(row=r + 1, column=2, value="GRAND TOTAL").font = Font(bold=True)
ws.cell(row=r + 1, column=7, value="=SUM(G6:G11)")
ws.cell(row=r + 3, column=1, value="Source: SAP export 2026-03-13. Q2 restated.")
ws.freeze_panes = "A6"

# --- Raw Export: blank + duplicate headers, mixed types, leading zeros, four date styles ---
ws = wb.create_sheet("Raw Export")
for col, name in enumerate(["order_id", "customer", None, "amount", "amount", "ordered_at", "qty", "active"], start=1):
    if name is not None:
        ws.cell(row=1, column=col, value=name)
ws.append(["00123", "Bauer GmbH", "n/a", 1299.99, "1299.99", datetime.datetime(2026, 1, 8, 14, 30), 3, True])
ws.append(["00124", "Zhang Ltd", "", 480, 480, datetime.date(2026, 1, 9), "4", False])
ws.append(["00125", "Ólafsson ehf", None, "N/A", None, "2026-01-10", 2, "yes"])
ws.append(["00126", "Müller & Co", "  ", 9999999999999999, 0.1 + 0.2, 45678, None, None])
ws.append(["00127", "Açaí SA 🌴", "x", -55.5, "-55,50", datetime.datetime(2026, 1, 12, 9, 5), 1, True])
ws.column_dimensions["C"].hidden = True

# --- Inventory: numbers stored as text, error values, a formula with no cached result ---
ws = wb.create_sheet("Inventory")
ws.append(["sku", "location", "on_hand", "reorder_at", "unit_cost", "status"])
ws.append(["SKU-001", "WH-A", "42", 20, 12.5, '=IF(C2<D2,"REORDER","OK")'])
ws.append(["SKU-002", "WH-A", 7, 20, "#N/A", "REORDER"])
ws.append(["SKU-003", "WH-B", 0, 5, 3.99, "#DIV/0!"])
ws.append(["SKU-004", None, 156, 50, 0.0001, "OK"])
ws.append([None, None, None, None, None, None])
ws.append(["SKU-005", "WH-C", 88, 30, 45.0, "OK"])
for row in ws.iter_rows(min_row=2, min_col=5, max_col=5):
    row[0].number_format = '"€"#,##0.0000'

# --- Timesheet: header on row 2, unit row under it, ragged trailing columns ---
ws = wb.create_sheet("Timesheet 2026")
ws["A1"] = "Week commencing 2026-02-02"
ws.append([])
ws.append(["employee", "mon", "tue", "wed", "thu", "fri", "notes"])
ws.append([None, "hrs", "hrs", "hrs", "hrs", "hrs", None])
ws.append(["E. Novak", 8, 8, 7.5, 8, 4, "left early Fri"])
ws.append(["F. Adeyemi", 8, 0, 8, 8, 8, "sick Tue"])
ws.append(["G. Petrov", 6, 6, 6, 6, 6, 'quoted "part time", 0.75 FTE'])
ws.append(["H. Silva", 8, 8, 8, 8, 8, "line one\nline two"])

# --- Headers Only: no data rows at all ---
ws = wb.create_sheet("Headers Only")
ws.append(["col_a", "col_b", "col_c"])

# --- Notes: freeform text, nothing tabular ---
ws = wb.create_sheet("Notes")
ws["B3"] = "Ignore this tab — scratch space."
ws["D9"] = 42
ws["A17"] = "https://example.invalid/report"

# --- Long Tail: 2000 rows, for eyeballing read performance ---
ws = wb.create_sheet("Long Tail")
ws.append(["idx", "bucket", "value", "ts"])
for i in range(1, 2001):
    ws.append([i, f"b{i % 7}", (i * 37) % 1000 / 10, datetime.datetime(2026, 1, 1) + datetime.timedelta(hours=i)])

wb.save(OUT)
print(f"wrote {OUT}")
for name in wb.sheetnames:
    sheet = wb[name]
    print(f"  {name:<16} {sheet.max_row} rows x {sheet.max_column} cols")
