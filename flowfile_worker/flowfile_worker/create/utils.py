from functools import partial
from random import randint

import polars as pl
from faker import Faker

from shared.dtype_utils import convert_to_string, create_pl_df_type_save, standardize_col_dtype

__all__ = ["convert_to_string", "create_fake_data", "create_pl_df_type_save", "standardize_col_dtype"]


def create_fake_data(n_records: int = 1000) -> pl.DataFrame:
    fake = Faker()
    selector = partial(randint, 0)
    min_range = partial(min, n_records)
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
    for _i in range(n_records):
        name = generate_name()
        data.append(
            dict(
                ID=randint(1, 1000000),
                Name=name,
                Address=generate_address(),
                City=cities[selector(min_range(7000)) - 1],
                Email=generate_email(name),
                Phone=generate_phone_number(),
                DOB=dob[selector(min_range(100_000)) - 1],
                Work=companies[selector(min_range(100_000)) - 1],
                Zipcode=zipcodes[selector(min_range(200_000)) - 1],
                Country=countries[selector(min_range(50)) - 1],
            )
        )

    return pl.DataFrame(data)
