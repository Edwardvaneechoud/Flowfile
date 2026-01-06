"""Password hashing and verification utilities."""

import re

from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Password requirements
PASSWORD_MIN_LENGTH = 8
PASSWORD_REQUIREMENTS = {
    "min_length": PASSWORD_MIN_LENGTH,
    "require_number": True,
    "require_special": True,
    "special_chars": "!@#$%^&*()_+-=[]{}|;:,.<>?"
}


class PasswordValidationError(Exception):
    """Raised when password doesn't meet requirements."""
    pass


def validate_password(password: str) -> tuple[bool, str]:
    """
    Validate password against security requirements.

    Requirements:
    - Minimum 8 characters
    - At least one number
    - At least one special character

    Args:
        password: The plain text password to validate

    Returns:
        Tuple of (is_valid, error_message)
    """
    if len(password) < PASSWORD_MIN_LENGTH:
        return False, f"Password must be at least {PASSWORD_MIN_LENGTH} characters long"

    if not re.search(r'\d', password):
        return False, "Password must contain at least one number"

    if not re.search(r'[!@#$%^&*()_+\-=\[\]{}|;:,.<>?]', password):
        return False, "Password must contain at least one special character (!@#$%^&*()_+-=[]{}|;:,.<>?)"

    return True, ""


def validate_password_or_raise(password: str) -> None:
    """
    Validate password and raise exception if invalid.

    Args:
        password: The plain text password to validate

    Raises:
        PasswordValidationError: If password doesn't meet requirements
    """
    is_valid, error_message = validate_password(password)
    if not is_valid:
        raise PasswordValidationError(error_message)


def verify_password(plain: str, hashed: str) -> bool:
    """
    Verify a plain password against a hashed password.

    Args:
        plain: The plain text password
        hashed: The hashed password to verify against

    Returns:
        True if the password matches, False otherwise
    """
    return pwd_context.verify(plain, hashed)


def get_password_hash(password: str) -> str:
    """
    Hash a plain text password.

    Args:
        password: The plain text password to hash

    Returns:
        The hashed password
    """
    return pwd_context.hash(password)
