# test_db.py
from sqlalchemy import text
from database import engine

with engine.connect() as conn:
    v = conn.execute(text("SELECT VERSION()")).scalar()
    print("Connected OK:", v)
