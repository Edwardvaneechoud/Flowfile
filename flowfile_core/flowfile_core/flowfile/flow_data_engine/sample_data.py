from functools import partial
from math import ceil
from random import randint

import polars as pl
from faker import Faker


def create_fake_data(n_records: int = 1000, optimized: bool = True) -> pl.DataFrame:
    """

    Args:
        n_records (): Number of records to return
        optimized (): Indicator if creation should be optimized, will result in more identical rows when True

    Returns:
        pl.DataFrame
    """
    fake = Faker()
    selector = partial(randint, 0)

    max_n_records = min(10_000, n_records) if optimized else n_records

    min_range = partial(min, max_n_records)
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
    sales_data = [fake.random_int(0, 1000) for _ in range(max_n_records)]

    def generate_name():
        return f"{first_names[selector(min_range(100_000))-1]} {last_names[selector(min_range(50_000))-1]}"

    def generate_address():
        return f"{randint(100, 999)} {street_names[selector(min_range(100000))-1]}"

    def generate_email(name):
        return f"{name.lower().replace(' ', '_')}.{randint(1, 99)}@{domain_names[selector(10)-1]}"

    def generate_phone_number():
        return fake.phone_number()

    data = []
    for i in range(max_n_records):
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
                sales_data=sales_data[selector(max_n_records) - 1],
            )
        )
    if max_n_records < n_records:
        n_duplicates: int = ceil(n_records / max_n_records)
        output = []
        for _ in range(n_duplicates):
            output.extend(data)
        data = output[:n_records]

    return pl.DataFrame(data)


