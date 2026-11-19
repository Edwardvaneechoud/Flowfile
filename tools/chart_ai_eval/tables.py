"""The three made-up tables the evaluation charts: tidy, messy and wide.

Each builder is deterministic for its seed, so a report from one run can be compared with
the next. The data is invented; no column or value comes from a real source.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from datetime import date, timedelta

import polars as pl

TABLE_KEYS: tuple[str, ...] = ("tidy", "messy", "wide")

_PRODUCTS: dict[str, list[tuple[str, float]]] = {
    "Electronics": [("Laptop", 899.0), ("Monitor", 229.0), ("Headphones", 79.0), ("Phone case", 19.0)],
    "Furniture": [("Desk", 349.0), ("Office chair", 189.0), ("Bookshelf", 129.0), ("Lamp", 39.0)],
    "Office": [("Notebook", 4.5), ("Pen set", 9.0), ("Stapler", 12.0), ("Printer paper", 7.5)],
    "Toys": [("Puzzle", 15.0), ("Board game", 32.0), ("Kite", 18.0), ("Building blocks", 45.0)],
    "Garden": [("Hose", 27.0), ("Shovel", 22.0), ("Planter", 14.0), ("Seeds", 3.5)],
}
_REGIONS = ["East", "North", "South", "West"]
_REGION_WEIGHTS = [3, 2, 2, 4]


def _day(rng: random.Random, start: date, end: date) -> date:
    return start + timedelta(days=rng.randrange((end - start).days + 1))


def build_tidy(seed: int = 7, rows: int = 2400) -> pl.DataFrame:
    """Sales orders with a real date column, clean names and few categories."""
    rng = random.Random(seed)
    items = [(cat, name, price) for cat, products in _PRODUCTS.items() for name, price in products]
    data: dict[str, list] = {k: [] for k in ("order_date", "region", "category", "product", "quantity")}
    data.update(unit_price=[], revenue=[], is_return=[])
    for _ in range(rows):
        category, product, base = rng.choice(items)
        quantity = rng.choices(range(1, 9), weights=[30, 22, 15, 10, 8, 6, 5, 4])[0]
        unit_price = round(base * rng.uniform(0.9, 1.1), 2)
        data["order_date"].append(_day(rng, date(2023, 1, 1), date(2025, 12, 31)))
        data["region"].append(rng.choices(_REGIONS, weights=_REGION_WEIGHTS)[0])
        data["category"].append(category)
        data["product"].append(product)
        data["quantity"].append(quantity)
        data["unit_price"].append(unit_price)
        data["revenue"].append(round(quantity * unit_price, 2))
        data["is_return"].append(rng.random() < 0.06)
    return pl.DataFrame(
        data,
        schema={
            "order_date": pl.Date,
            "region": pl.String,
            "category": pl.String,
            "product": pl.String,
            "quantity": pl.Int64,
            "unit_price": pl.Float64,
            "revenue": pl.Float64,
            "is_return": pl.Boolean,
        },
    )


_FIRST = ["Anna", "Bram", "Chloe", "Daan", "Eva", "Finn", "Greta", "Hugo", "Iris", "Jens"]
_FIRST += ["Kim", "Lars", "Mila", "Noah", "Olga", "Pim", "Quinn", "Rosa", "Sven", "Tess"]
_LAST = ["Bakker", "Claes", "Dekker", "Engel", "Fischer", "Gomez", "Hendriks", "Ivanov", "Jansen"]
_LAST += ["Kok", "Lambert", "Martin", "Nowak", "Olsen", "Peeters", "Quist", "Roth", "Smit"]
_LAST += ["Torres", "Unger", "Visser", "Weber", "Yilmaz", "Zwart", "Mertens"]


def build_messy(seed: int = 11, rows: int = 1500) -> pl.DataFrame:
    """Orders the way real exports look: cryptic names, a text date, Y/N text, a ``count`` column."""
    rng = random.Random(seed)
    names = [f"{f} {last}" for f in _FIRST for last in _LAST]
    customers = rng.sample(names, 420)
    zips = rng.sample(range(1000, 10000), 650)
    ids = list(range(100001, 100001 + rows))
    rng.shuffle(ids)
    data: dict[str, list] = {
        k: []
        for k in (
            "ORD_ID",
            "dt_ord",
            "cust_seg_cd",
            "Cust_Name",
            "AMT_NET_EUR",
            "disc_pct",
            "ret_flg",
            "count",
            "zip_cd",
            "prio_lvl",
            "ShipMode",
            "rgn",
        )
    }
    for i in range(rows):
        day = _day(rng, date(2024, 1, 1), date(2025, 6, 30))
        count = rng.randint(1, 12)
        data["ORD_ID"].append(ids[i])
        data["dt_ord"].append(day.strftime("%d/%m/%Y"))
        data["cust_seg_cd"].append(rng.choices(["RTL", "WHS", "ONL", "GOV"], weights=[5, 2, 4, 1])[0])
        data["Cust_Name"].append(rng.choice(customers))
        data["AMT_NET_EUR"].append(None if rng.random() < 0.03 else round(count * rng.uniform(8, 120), 2))
        data["disc_pct"].append(None if rng.random() < 0.1 else round(rng.uniform(0, 0.35), 3))
        data["ret_flg"].append("Y" if rng.random() < 0.08 else "N")
        data["count"].append(count)
        data["zip_cd"].append(rng.choice(zips))
        data["prio_lvl"].append(rng.choices([1, 2, 3, 4], weights=[1, 3, 4, 2])[0])
        data["ShipMode"].append(None if rng.random() < 0.05 else rng.choice(["AIR", "SEA", "ROAD"]))
        data["rgn"].append(rng.choice(["N", "E", "S", "W"]))
    return pl.DataFrame(
        data,
        schema={
            "ORD_ID": pl.Int64,
            "dt_ord": pl.String,
            "cust_seg_cd": pl.String,
            "Cust_Name": pl.String,
            "AMT_NET_EUR": pl.Float64,
            "disc_pct": pl.Float64,
            "ret_flg": pl.String,
            "count": pl.Int64,
            "zip_cd": pl.Int64,
            "prio_lvl": pl.Int64,
            "ShipMode": pl.String,
            "rgn": pl.String,
        },
    )


_CITIES = {
    "Netherlands": ["Amsterdam", "Rotterdam", "Utrecht"],
    "Belgium": ["Antwerp", "Ghent"],
    "Germany": ["Berlin", "Hamburg", "Cologne"],
    "France": ["Lyon", "Lille"],
    "Spain": ["Madrid", "Valencia"],
}
_FORMATS = ["Mall", "High street", "Outlet", "Airport"]

_WIDE_METRICS: dict[str, Callable[[random.Random, float], float | int]] = {
    "sales_amt": lambda r, s: round(s * r.uniform(40_000, 60_000), 2),
    "units_sold": lambda r, s: int(s * r.uniform(1800, 2600)),
    "returns_cnt": lambda r, s: int(s * r.uniform(40, 120)),
    "footfall": lambda r, s: int(s * r.uniform(9000, 15000)),
    "transactions": lambda r, s: int(s * r.uniform(2500, 4000)),
    "avg_basket": lambda r, s: round(r.uniform(12, 32), 2),
    "conversion_rate": lambda r, s: round(r.uniform(0.15, 0.4), 3),
    "online_share": lambda r, s: round(r.uniform(0.05, 0.3), 3),
    "staff_count": lambda r, s: int(s * r.uniform(8, 14)),
    "staff_hours": lambda r, s: round(s * r.uniform(1200, 2000), 1),
    "energy_kwh": lambda r, s: round(s * r.uniform(9000, 16000), 1),
    "water_m3": lambda r, s: round(s * r.uniform(40, 90), 1),
    "waste_kg": lambda r, s: round(s * r.uniform(600, 1400), 1),
    "nps_score": lambda r, s: int(r.uniform(-10, 70)),
    "complaints_cnt": lambda r, s: r.randint(0, 25),
    "stockouts_cnt": lambda r, s: r.randint(0, 40),
    "inventory_value": lambda r, s: round(s * r.uniform(150_000, 260_000), 2),
    "markdown_amt": lambda r, s: round(s * r.uniform(1000, 6000), 2),
    "promo_spend": lambda r, s: round(s * r.uniform(500, 4000), 2),
    "loyalty_signups": lambda r, s: r.randint(20, 400),
    "gift_card_sales": lambda r, s: round(r.uniform(200, 5000), 2),
    "avg_wait_min": lambda r, s: round(r.uniform(0.5, 9.0), 1),
    "self_checkout_share": lambda r, s: round(r.uniform(0.0, 0.6), 3),
    "shrinkage_amt": lambda r, s: round(s * r.uniform(200, 1500), 2),
    "temp_avg_c": lambda r, s: round(r.uniform(-2, 28), 1),
    "rain_days": lambda r, s: r.randint(0, 20),
    "local_events": lambda r, s: r.randint(0, 6),
    "delivery_orders": lambda r, s: int(s * r.uniform(50, 400)),
    "click_collect_orders": lambda r, s: int(s * r.uniform(30, 300)),
}


def build_wide(seed: int = 13, stores: int = 40) -> pl.DataFrame:
    """Store-month figures with 45 columns of mixed types, few of which matter to one question."""
    rng = random.Random(seed)
    managers = [f"{f} {last}" for f in _FIRST for last in _LAST]
    pairs = [(country, city) for country, cities in _CITIES.items() for city in cities]
    months = [date(2024 + m // 12, m % 12 + 1, 1) for m in range(24)]
    rows: list[dict] = []
    for store in range(1, stores + 1):
        country, city = pairs[(store - 1) % len(pairs)]
        store_format = rng.choice(_FORMATS)
        scale = {"Mall": 1.3, "High street": 1.0, "Outlet": 0.8, "Airport": 0.6}[store_format] * rng.uniform(0.8, 1.2)
        fixed = {
            "store_id": 500 + store,
            "store_name": f"{city} {store_format} {store:02d}",
            "city": city,
            "country": country,
            "store_format": store_format,
            "manager_name": rng.choice(managers),
            "region_code": f"R{(store - 1) % 6 + 1}",
            "is_flagship": rng.random() < 0.15,
            "has_parking": store_format in ("Mall", "Outlet") or rng.random() < 0.2,
            "opened_on": _day(rng, date(2005, 1, 1), date(2022, 12, 31)),
            "sq_meters": int(scale * rng.uniform(600, 1400)),
            "rent_eur": round(scale * rng.uniform(15_000, 45_000), 2),
            "checkout_lanes": rng.randint(3, 16),
            "parking_spaces": rng.randint(0, 400),
            "competitor_count": rng.randint(0, 9),
        }
        for month in months:
            row = {**fixed, "report_month": month}
            for name, make in _WIDE_METRICS.items():
                row[name] = make(rng, scale)
            rows.append(row)
    frame = pl.DataFrame(rows, infer_schema_length=None)
    return frame.with_columns(pl.col("opened_on", "report_month").cast(pl.Date))


BUILDERS: dict[str, Callable[[], pl.DataFrame]] = {"tidy": build_tidy, "messy": build_messy, "wide": build_wide}


def build(key: str) -> pl.DataFrame:
    """The table for ``key``, one of ``TABLE_KEYS``."""
    return BUILDERS[key]()
