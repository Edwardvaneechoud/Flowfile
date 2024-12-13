import platform
import os

from databases import DatabaseURL
from passlib.context import CryptContext
from starlette.config import Config
from starlette.datastructures import Secret


def is_running_in_docker() -> bool:
    """
    Check if the current process is running in a Docker container.
    Returns: bool

    """
    return False


def get_default_worker_url():
    # Check for Docker environment first
    worker_host = os.getenv('WORKER_HOST', None)
    print('worker host', worker_host)
    if worker_host:
        return f"http://{worker_host}:63579"

    # Fall back to default behavior
    if platform.system() == "Windows":
        return "http://127.0.0.1:63579"
    else:
        return "http://0.0.0.0:63579"


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
IS_RUNNING_IN_DOCKER = is_running_in_docker()

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 120
PWD_CONTEXT = CryptContext(schemes=["bcrypt"], deprecated="auto")
