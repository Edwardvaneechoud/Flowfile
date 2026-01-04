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
