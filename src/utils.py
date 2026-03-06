import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import os

@contextmanager
def get_db_connection():
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    try:
        yield conn
    finally:
        conn.close()

def build_select_query(columns: list) -> str:
    """Build parameterized SELECT query from column mappings"""
    source_columns = [col.source for col in columns]
    return f"SELECT {', '.join(source_columns)} FROM records ORDER BY id"
