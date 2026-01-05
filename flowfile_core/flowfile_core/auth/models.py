from pydantic import BaseModel, SecretStr


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: str | None = None


class User(BaseModel):
    username: str
    id: int | None = None
    email: str | None = None
    full_name: str | None = None
    disabled: bool | None = False
    is_admin: bool | None = False


class UserInDB(User):
    hashed_password: str


class UserCreate(BaseModel):
    """Model for creating a new user (admin only)"""
    username: str
    password: str
    email: str | None = None
    full_name: str | None = None
    is_admin: bool = False


class UserUpdate(BaseModel):
    """Model for updating a user (admin only)"""
    email: str | None = None
    full_name: str | None = None
    disabled: bool | None = None
    is_admin: bool | None = None
    password: str | None = None  # Optional password change


class SecretInput(BaseModel):
    name: str
    value: SecretStr


class Secret(SecretInput):
    user_id: str


class SecretInDB(BaseModel):
    id: str
    name: str
    encrypted_value: str
    user_id: str
