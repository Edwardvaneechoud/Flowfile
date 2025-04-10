
from pydantic import BaseModel, SecretStr
from typing import Optional, List


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None


class User(BaseModel):
    username: str
    id: Optional[int] = None
    email: Optional[str] = None
    full_name: Optional[str] = None
    disabled: Optional[bool] = False


class UserInDB(User):
    hashed_password: str


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
