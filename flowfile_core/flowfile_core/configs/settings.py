import platform

from databases import DatabaseURL
from passlib.context import CryptContext
from starlette.config import Config
from starlette.datastructures import Secret


def get_default_worker_url():
    if platform.system() == "Windows":
        return "http://127.0.0.1:8000"
    else:
        return "http://0.0.0.0:8000"


config = Config(".env")
DEBUG: bool = config("DEBUG", cast=bool, default=False)
DATABASE_URL: DatabaseURL = config("DRIVERNAME", cast=DatabaseURL, default='mysqldb')
MAX_CONNECTIONS_COUNT: int = config("MAX_CONNECTIONS_COUNT", cast=int, default=10)
MIN_CONNECTIONS_COUNT: int = config("MIN_CONNECTIONS_COUNT", cast=int, default=1)
HOST: str = config("HOST", cast=str, default="127.0.0.1")
PORT: int = config("PORT", cast=int, default=3306)
USER: str = config("MYSQL_USER", cast=str, default="root")
PWD: str = config("MYSQL_PASSWORD", cast=str, default="")
DB: str = config("DB", cast=str, default="")
DATABASE: str = config("MYSQL_DATABASE", cast=str, default="MYSQL_DATABASE")
SECRET_KEY: Secret = config("SECRET_KEY", cast=Secret, default='edward')
FILE_LOCATION = config("FILE_LOCATION", cast=str, default=".\\files\\")
AVAILABLE_RAM = config("AVAILABLE_RAM", cast=int, default=8)
WORKER_URL = config("WORKER_URL", cast=str, default=get_default_worker_url())


PORT = '7879'

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 120
PWD_CONTEXT = CryptContext(schemes=["bcrypt"], deprecated="auto")
