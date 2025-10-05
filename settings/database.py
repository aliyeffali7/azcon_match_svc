# database.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from settings import settings

PRIMARY_DB_URL = (
    f"{settings.DB_ENGINE}://{settings.DB_USER}:{settings.DB_PASSWORD}"
    f"@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    f"?charset=utf8mb4"
)

engine = create_engine(
    PRIMARY_DB_URL,
    echo=settings.SQL_ECHO,
    pool_size=settings.POOL_SIZE,
    pool_pre_ping=True,
    pool_recycle=settings.POOL_RECYCLE,
    future=True,
)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
