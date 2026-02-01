"""Tests for auth/password module - password validation functions."""

import pytest

from flowfile_core.auth.password import (
    PASSWORD_MIN_LENGTH,
    PASSWORD_REQUIREMENTS,
    PasswordValidationError,
    get_password_hash,
    validate_password,
    validate_password_or_raise,
    verify_password,
)


class TestPasswordConstants:
    """Test password configuration constants."""

    def test_min_length(self):
        assert PASSWORD_MIN_LENGTH == 8

    def test_requirements_keys(self):
        assert "min_length" in PASSWORD_REQUIREMENTS
        assert "require_number" in PASSWORD_REQUIREMENTS
        assert "require_special" in PASSWORD_REQUIREMENTS
        assert "special_chars" in PASSWORD_REQUIREMENTS


class TestValidatePassword:
    """Test validate_password function."""

    def test_valid_password(self):
        is_valid, msg = validate_password("MyP@ssw0rd!")
        assert is_valid is True
        assert msg == ""

    def test_too_short(self):
        is_valid, msg = validate_password("Sh0rt!")
        assert is_valid is False
        assert "at least 8 characters" in msg

    def test_no_number(self):
        is_valid, msg = validate_password("NoNumber!@#")
        assert is_valid is False
        assert "at least one number" in msg

    def test_no_special_char(self):
        is_valid, msg = validate_password("NoSpecial123")
        assert is_valid is False
        assert "at least one special character" in msg

    def test_all_requirements_met(self):
        is_valid, msg = validate_password("GoodPass1!")
        assert is_valid is True

    def test_exactly_min_length(self):
        is_valid, msg = validate_password("Pass1!ab")
        assert is_valid is True

    def test_one_below_min_length(self):
        is_valid, msg = validate_password("Pa1!abc")
        assert is_valid is False

    def test_various_special_chars(self):
        special_chars = "!@#$%^&*()_+-=[]{}|;:,.<>?"
        for char in special_chars:
            pw = f"Password1{char}"
            is_valid, _ = validate_password(pw)
            assert is_valid is True, f"Special char '{char}' should be accepted"


class TestValidatePasswordOrRaise:
    """Test validate_password_or_raise function."""

    def test_valid_password_no_raise(self):
        # Should not raise
        validate_password_or_raise("ValidPass1!")

    def test_invalid_password_raises(self):
        with pytest.raises(PasswordValidationError):
            validate_password_or_raise("short")

    def test_invalid_password_error_message(self):
        with pytest.raises(PasswordValidationError, match="at least 8 characters"):
            validate_password_or_raise("Sh0rt!")


class TestVerifyPassword:
    """Test verify_password function."""

    def test_verify_correct(self):
        hashed = get_password_hash("TestPass1!")
        assert verify_password("TestPass1!", hashed) is True

    def test_verify_incorrect(self):
        hashed = get_password_hash("TestPass1!")
        assert verify_password("WrongPass1!", hashed) is False


class TestGetPasswordHash:
    """Test get_password_hash function."""

    def test_returns_string(self):
        result = get_password_hash("TestPass1!")
        assert isinstance(result, str)

    def test_returns_bcrypt_hash(self):
        result = get_password_hash("TestPass1!")
        assert result.startswith("$2b$")

    def test_different_salts(self):
        h1 = get_password_hash("SamePass1!")
        h2 = get_password_hash("SamePass1!")
        assert h1 != h2  # Salt should differ
