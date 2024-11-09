from faker import Faker
from functools import partial
from random import randint
import polars as pl
from typing import List, Dict, Any, Generator


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
    sales_data = [fake.random_int(0, 1000) for _ in range(n_records)]

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
            Country=countries[selector(min_range(50))-1],
            sales_data=sales_data[selector(n_records)-1]
        ))

    return pl.DataFrame(data)



def create_fake_data_raw(n_records: int = 1000, col_selection: List[str] = None) -> Generator[Dict[str, Any], None, None]:
    fake = Faker()
    selector = partial(randint, 0)

    # Caching static data to avoid regenerating it unnecessarily
    cities = partial(fake.city)
    companies = partial(fake.company)
    zipcodes = partial(fake.zipcode)
    countries = partial(fake.country)
    street_names = partial(fake.street_name)
    dob = partial(fake.date_of_birth)
    first_names = partial(fake.first_name)
    last_names = partial(fake.last_name)
    domain_names = [fake.domain_name() for _ in range(10)]  # Pre-generate a small list
    sales_data = lambda: fake.random_int(0, 1000)

    def generate_name():
        return f"{first_names()} {last_names()}"

    def generate_address():
        return f"{randint(100, 999)} {street_names()}"

    def generate_email(_name):
        return f"{_name.lower().replace(' ', '_')}.{randint(1, 99)}@{domain_names[selector(10)-1]}"

    def generate_phone_number():
        return fake.phone_number()

    # Default columns if no selection is provided
    all_columns = {
        "ID": lambda: randint(1, 1000000),
        "Name": generate_name,
        "Address": generate_address,
        "City": cities,
        "Email": lambda name: generate_email(name),
        "Phone": generate_phone_number,
        "DOB": dob,
        "Work": companies,
        "Zipcode": zipcodes,
        "Country": countries,
        "sales_data": sales_data
    }

    # Filter the available columns based on col_selection
    if col_selection is not None:
        all_columns = {col: all_columns[col] for col in col_selection if col in all_columns}

    # Use a generator to yield one record at a time
    for _ in range(n_records):
        record = {}
        for col, generator in all_columns.items():
            if col == "Email":  # Email depends on the Name column
                name = record.get("Name", generate_name())
                record["Email"] = generate_email(name)
            else:
                record[col] = generator()
        yield record


def write_fake_data():
    df = create_fake_data()
    df.write_parquet('backend/tests/data/fake_data.parquet')
    df.write_csv('backend/tests/data/fake_data.csv')
    df.write_excel('backend/tests/data/fake_data.xlsx')