import polars as pl
import numpy as np
import random
import string
from typing import Tuple, List, Dict, Any
from flowfile_worker.polars_fuzzy_match.models import FuzzyMapping


def introduce_typos(text: str, error_rate: float = 0.2) -> str:
    """
    Introduces random typos in text to simulate fuzzy matching scenarios.

    Args:
        text: Original text
        error_rate: Probability of each character being altered

    Returns:
        Text with typos
    """
    if not text or error_rate <= 0:
        return text

    result = ""
    for char in text:
        if random.random() < error_rate:
            # Choose type of error: insert, delete, replace
            error_type = random.choice(["insert", "delete", "replace"])

            if error_type == "insert":
                result += random.choice(string.ascii_letters) + char
            elif error_type == "delete" and len(text) > 3:
                # Skip this character (but ensure we don't delete too much)
                pass
            elif error_type == "replace":
                result += random.choice(string.ascii_letters)
            else:
                result += char
        else:
            result += char

    return result


# Data generation functions
def generate_large_scale_data(
        size: int = 10000,
        match_rate: float = 0.7,
        error_rate: float = 0.2
) -> Dict[str, Dict[str, List[Any]]]:
    """
    Generates raw data for large-scale test scenarios.

    Args:
        size: Number of rows to generate
        match_rate: Proportion of rows that should have a match
        error_rate: Rate of typos in matching fields

    Returns:
        Dictionary with 'left_data' and 'right_data' dictionaries
    """
    # Generate company names
    companies = [f"Company {i}" for i in range(size)]

    # For right dataframe, some will match with typos, some won't match at all
    right_companies = []
    for i in range(size):
        if random.random() < match_rate:
            # This one should match with possible typos
            right_companies.append(introduce_typos(companies[i], error_rate))
        else:
            # This one shouldn't match
            right_companies.append(f"Different Company {i + size}")

    # Generate other fields
    addresses = [f"{random.randint(1, 9999)} Main St, City {i}" for i in range(size)]
    right_addresses = [
        introduce_typos(addr, error_rate) if random.random() < match_rate else f"Different Address {i}"
        for i, addr in enumerate(addresses)
    ]

    # Generate countries (not very unique - limited set)
    countries = ["USA", "Canada", "UK", "Germany", "France", "Italy", "Japan", "China", "India", "Brazil"]
    left_countries = [countries[i % len(countries)] for i in range(size)]
    right_countries = []
    for i in range(size):
        if random.random() < match_rate:
            # This one should match with possible typos
            country = left_countries[i]
            if random.random() < error_rate:
                # Add some typos occasionally
                country = introduce_typos(country, error_rate)
            right_countries.append(country)
        else:
            # Pick a different country
            different_country_index = (i % len(countries) + random.randint(1, len(countries) - 1)) % len(countries)
            right_countries.append(countries[different_country_index])

    # Create left dataframe data
    left_data = {
        "id": list(range(1, size + 1)),
        "company_name": companies,
        "address": addresses,
        "country": left_countries
    }

    # Create right dataframe data
    right_data = {
        "id": list(range(1001, size + 1001)),
        "organization": right_companies,
        "location": right_addresses,
        "country_code": right_countries
    }

    return {
        "left_data": left_data,
        "right_data": right_data
    }

def generate_edge_case_data() -> Dict[str, Dict[str, Dict[str, List[Any]]]]:
    """
    Generates raw data for different edge case scenarios.

    Returns:
        Dictionary mapping scenario names to data dictionaries
    """
    edge_case_data = {}

    # Case 1: Empty dataframes
    edge_case_data["empty"] = {
        "left_data": {"company_name": [], "address": []},
        "right_data": {"organization": [], "location": []}
    }

    # Case 2: One-to-many matches
    edge_case_data["one_to_many"] = {
        "left_data": {
            "id": [1],
            "company_name": ["ACME Corporation"],
            "address": ["100 Main St"]
        },
        "right_data": {
            "id": [101, 102, 103, 104, 105],
            "organization": ["ACME Corp", "ACME Corp.", "ACME Inc", "ACME Corporation", "Completely Different"],
            "location": ["100 Main Street", "100 Main St", "100 Main", "Different Address", "Different Address 2"]
        }
    }

    # Case 3: Many-to-one matches
    edge_case_data["many_to_one"] = {
        "left_data": {
            "id": [1, 2, 3, 4, 5],
            "company_name": ["ACME Corp", "ACME Corp.", "ACME Inc", "ACME Corporation", "Completely Different"],
            "address": ["100 Main Street", "100 Main St", "100 Main", "Different Address", "Different Address 2"]
        },
        "right_data": {
            "id": [101],
            "organization": ["ACME Corporation"],
            "location": ["100 Main St"]
        }
    }

    # Case 4: Multiple fuzzy criteria with varying thresholds
    edge_case_data["multi_criteria"] = {
        "left_data": {
            "id": [1, 2, 3, 4, 5],
            "name": ["John Smith", "Jane Doe", "Bob Johnson", "Alice Brown", "David Miller"],
            "email": ["jsmith@example.com", "jane.doe@example.com", "bob.j@example.com", "alice@example.com",
                      "david@example.com"],
            "phone": ["555-1234", "555-5678", "555-9012", "555-3456", "555-7890"]
        },
        "right_data": {
            "id": [101, 102, 103, 104, 105],
            "full_name": ["John Smith", "Jane Doe", "Robert Johnson", "Alice B.", "Dave Miller"],
            "contact_email": ["john.smith@example.com", "janedoe@example.com", "bob.johnson@example.com",
                              "alice@different.com", "d.miller@example.com"],
            "contact_phone": ["555-1234", "555-5678", "555-9999", "555-3456", "555-7890"]
        }
    }

    # Case 5: Null values in key columns
    edge_case_data["null_values"] = {
        "left_data": {
            "id": [1, 2, 3, 4, 5],
            "company_name": ["Company A", None, "Company C", "Company D", "Company E"],
            "address": ["Address 1", "Address 2", None, "Address 4", "Address 5"]
        },
        "right_data": {
            "id": [101, 102, 103, 104, 105],
            "organization": ["Company A", "Company B", "Company C", None, "Company E"],
            "location": ["Address 1", "Address 2", "Address 3", "Address 4", None]
        }
    }

    return edge_case_data


# Create LazyFrames from raw data
def create_lazy_frames(data: Dict[str, List[Any]]) -> pl.LazyFrame:
    """
    Creates a LazyFrame from dictionary data.

    Args:
        data: Dictionary with column names as keys and data as values

    Returns:
        pl.LazyFrame
    """
    return pl.DataFrame(data).lazy()


# Create FuzzyMappings
def create_fuzzy_mappings(
        left_col: str,
        right_col: str,
        fuzzy_type: str = "jaro_winkler",
        threshold_score: float = 20.0,
        perc_unique: float = 1.0
) -> FuzzyMapping:
    """
    Creates a FuzzyMapping object.

    Args:
        left_col: Column name in left dataframe
        right_col: Column name in right dataframe
        fuzzy_type: Type of fuzzy matching algorithm
        threshold_score: Threshold score for fuzzy matching (0-100)
        perc_unique: Percentage uniqueness factor

    Returns:
        FuzzyMapping object
    """
    return FuzzyMapping(
        left_col=left_col,
        right_col=right_col,
        fuzzy_type=fuzzy_type,
        threshold_score=threshold_score,
        perc_unique=perc_unique
    )


def create_fuzzy_maps():
    # Create the fuzzy mappings
    fuzzy_mappings = [
        create_fuzzy_mappings(
            left_col="company_name",
            right_col="organization",
            fuzzy_type="levenshtein",
            threshold_score=80.0,  # 20% threshold corresponds to 0.8 reversed (80% similarity)
            perc_unique=1.0
        ),
        create_fuzzy_mappings(
            left_col="address",
            right_col="location",
            fuzzy_type="levenshtein",
            threshold_score=80.0,  # 20% threshold corresponds to 0.8 reversed (80% similarity)
            perc_unique=1.2
        ),
        create_fuzzy_mappings(
            left_col="country",
            right_col="country_code",
            fuzzy_type="jaro_winkler",
            threshold_score=90.0,  # Higher threshold for country codes as they should be more exact
            perc_unique=0.5  # Lower uniqueness factor since countries are not very unique
        )
    ]
    return fuzzy_mappings


# Combined utility functions (for backward compatibility)
def create_test_data(
        size: int = 10000,
        match_rate: float = 0.7,
        error_rate: float = 0.2
) -> Tuple[pl.LazyFrame, pl.LazyFrame, List[FuzzyMapping]]:
    """
    Creates large-scale test data for performance testing.

    Args:
        size: Number of rows in each dataframe
        match_rate: Proportion of rows that should have a match
        error_rate: Rate of typos in matching fields

    Returns:
        Tuple of (left_df, right_df, fuzzy_mappings)
    """
    # Generate the data
    data = generate_large_scale_data(size, match_rate, error_rate)

    # Create the LazyFrames
    left_df = create_lazy_frames(data["left_data"])
    right_df = create_lazy_frames(data["right_data"])

    return left_df, right_df, create_fuzzy_maps()


def create_edge_case_test_data() -> Dict[str, Tuple[pl.LazyFrame, pl.LazyFrame, List[FuzzyMapping]]]:
    """
    Creates a dictionary of edge case test scenarios.

    Returns:
        Dictionary mapping scenario name to (left_df, right_df, fuzzy_mappings)
    """
    # Generate the edge case data
    edge_case_data = generate_edge_case_data()

    # Process each edge case
    edge_cases = {}
    for case_name, data in edge_case_data.items():
        # Create the LazyFrames
        left_df = create_lazy_frames(data["left_data"])
        right_df = create_lazy_frames(data["right_data"])

        # Create appropriate fuzzy mappings based on the case
        if case_name == "empty":
            fuzzy_mappings = [
                create_fuzzy_mappings(
                    left_col="company_name",
                    right_col="organization",
                    threshold_score=20.0
                )
            ]
        elif case_name == "one_to_many" or case_name == "many_to_one":
            fuzzy_mappings = [
                create_fuzzy_mappings(
                    left_col="company_name",
                    right_col="organization",
                    threshold_score=30.0
                )
            ]
        elif case_name == "multi_criteria":
            fuzzy_mappings = [
                create_fuzzy_mappings(
                    left_col="name",
                    right_col="full_name",
                ),
                create_fuzzy_mappings(
                    left_col="email",
                    right_col="contact_email",
                    fuzzy_type="levenshtein",
                    threshold_score=30.0,
                ),
                create_fuzzy_mappings(
                    left_col="phone",
                    right_col="contact_phone",
                    fuzzy_type="exact",
                    threshold_score=0.0,
                )
            ]
        elif case_name == "null_values":
            fuzzy_mappings = [
                create_fuzzy_mappings(
                    left_col="company_name",
                    right_col="organization",
                    threshold_score=20.0
                )
            ]
        else:
            fuzzy_mappings = [
                create_fuzzy_mappings(
                    left_col="company_name",
                    right_col="organization"
                )
            ]

        edge_cases[case_name] = (left_df, right_df, fuzzy_mappings)

    return edge_cases


def generate_small_fuzzy_test_data_left() -> pl.DataFrame:
    """
    Generates a small, predictable test dataset with data designed for fuzzy matching challenges.

    Returns:
        LazyFrame with left side test data
    """
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "company_name": ["Apple Inc.", "Microsft", "Amazon", "Gogle", "Facebok"],
        "address": ["1 Apple Park", "One Microsoft Way", "410 Terry Ave N", "1600 Amphitheatre", "1 Hacker Way"],
        "contact": ["Tim Cook", "Satya Ndella", "Andy Jessy", "Sundar Pichai", "Mark Zukerberg"]
    })


def generate_small_fuzzy_test_data_right() -> pl.DataFrame:
    """
    Generates a small, predictable test dataset with variations for fuzzy matching.

    Returns:
        LazyFrame with right side test data
    """
    return pl.DataFrame({
        "id": [101, 102, 103, 104, 105],
        "organization": ["Apple Incorporated", "Microsoft Corp", "Amazon.com Inc", "Google LLC", "Facebook Inc"],
        "location": ["Apple Park, Cupertino", "Microsoft Way, Redmond", "Terry Ave North, Seattle",
                     "Amphitheatre Pkwy, Mountain View", "Hacker Way, Menlo Park"],
        "ceo": ["Timothy Cook", "Satya Nadella", "Andy Jassy", "Sundar Pichai", "Mark Zuckerberg"]
    })


def generate_small_fuzzy_test_mappings() -> List[FuzzyMapping]:
    """
    Creates fuzzy mappings for the small test dataset.

    Returns:
        List of FuzzyMapping objects
    """
    return [
        create_fuzzy_mappings(
            left_col="company_name",
            right_col="organization",
            fuzzy_type="jaro_winkler",
            threshold_score=20.0
        ),
        create_fuzzy_mappings(
            left_col="contact",
            right_col="ceo",
            fuzzy_type="levenshtein",
            threshold_score=30.0
        )
    ]


def generate_small_fuzzy_test_data() -> Tuple[pl.DataFrame, pl.DataFrame, List[FuzzyMapping]]:
    """
    Generates small test data for fuzzy matching.
    """

    left_df = generate_small_fuzzy_test_data_left()
    right_df = generate_small_fuzzy_test_data_right()
    fuzzy_mappings = generate_small_fuzzy_test_mappings()
    return left_df, right_df, fuzzy_mappings