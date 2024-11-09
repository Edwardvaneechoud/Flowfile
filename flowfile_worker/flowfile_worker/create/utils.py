from faker import Faker
from functools import partial
from random import randint
import polars as pl
from typing import Iterable


def create_fake_data(n_records: int = 1000) -> pl.DataFrame:
    fake = Faker()
    selector = partial(randint,0)
    min_range = partial(min, n_records)
    # Pre-generation of static data
    cities = [fake.city() for _ in range(min_range(7000))]
    companies = [fake.company() for _ in range(min_range(100_000))]
    zipcodes = [fake.zipcode() for _ in range(min_range(200_000))]
    countries = [fake.country() for _ in range(min_range(50))]
    street_names = [fake.street_name() for _ in range(min_range(100000))]
    dob = [fake.date_of_birth() for _ in range(min_range(100_000))]
    first_names = [fake.first_name() for _ in range(min_range(100_000))]
    last_names = [fake.last_name() for _ in range(min_range(50_000))]
    domain_names = [fake.domain_name() for _ in range(10)]

    def generate_name():
        return f"{first_names[selector(min_range(100_000))-1]} {last_names[selector(min_range(50_000))-1]}"

    def generate_address():
        return f"{randint(100, 999)} {street_names[selector(min_range(100000))-1]}"

    def generate_email(name):
        return f"{name.lower().replace(' ', '_')}.{randint(1, 99)}@{domain_names[selector(10)-1]}"

    def generate_phone_number():
        return fake.phone_number()

    data = []
    for i in range(n_records):
        name = generate_name()
        data.append(dict(
            ID=randint(1, 1000000),
            Name=name,
            Address=generate_address(),
            City=cities[selector(min_range(7000))-1],
            Email=generate_email(name),
            Phone=generate_phone_number(),
            DOB=dob[selector(min_range(100_000))-1],
            Work=companies[selector(min_range(100_000))-1],
            Zipcode=zipcodes[selector(min_range(200_000))-1],
            Country=countries[selector(min_range(50))-1]
        ))

    return pl.DataFrame(data)


def convert_to_string(v):
    try:
        return str(v)
    except:
        return None


def standardize_col_dtype(vals):
    types = set(type(val) for val in vals)
    if len(types) == 1:
        return vals
    elif int in types and float in types:
        return vals
    else:
        return [convert_to_string(v) for v in vals]


def create_pl_df_type_save(raw_data: Iterable[Iterable], orient: str = 'row') -> pl.DataFrame:
    """
        orient : {'col', 'row'}, default None
        Whether to interpret two-dimensional data as columns or as rows. If None,
        the orientation is inferred by matching the columns and data dimensions. If
        this does not yield conclusive results, column orientation is used.
    :param raw_data: iterables with values
    :param orient:
    :return: polars dataframe
    """
    if orient == 'row':
        raw_data = zip(*raw_data)
    raw_data = [standardize_col_dtype(values) for values in raw_data]
    return pl.DataFrame(raw_data, orient='col')
