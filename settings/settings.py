# settings.py
from pydantic import BaseSettings, Field
from typing import Optional

class Settings(BaseSettings):
    # Primary DB
    DB_ENGINE: str = "mysql+pymysql"
    DB_HOST: str = "127.0.0.1"
    DB_PORT: int = 3306
    DB_NAME: str = "azcon_db"
    DB_USER: str = "root"
    DB_PASSWORD: str = "fluenT072315"

    SQL_ECHO: bool = False
    POOL_SIZE: int = 5
    POOL_RECYCLE: int = 280

    # Friend DB (optional)
    FRIEND_DB_URL: Optional[str] = None
    FRIEND_TABLE: Optional[str] = None
    COL_TEXT: Optional[str] = None
    COL_FLAG: Optional[str] = None
    COL_AMOUNT: Optional[str] = None
    COL_UNIT: Optional[str] = None
    COL_PRICE: Optional[str] = None

    RESULT_POST_URL: Optional[str] = None
    RESULT_POST_MODE: str = "multipart"

    GET_AUTH_TYPE: str = "none"
    POST_AUTH_TYPE: str = "none"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()
