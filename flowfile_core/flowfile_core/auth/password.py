"""Password hashing and verification utilities."""

from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


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
