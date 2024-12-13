import re


def camel_case_to_snake_case(text: str) -> str:
    # Use a regular expression to find capital letters and replace them with _ followed by the lowercase letter
    transformed_text = re.sub(r'(?<!^)(?=[A-Z])', '_', text).lower()
    return transformed_text

