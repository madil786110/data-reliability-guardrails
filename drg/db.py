import os
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

# Default config for local docker-compose
DB_CONFIG = {
    "dbname": "drg_db",
    "user": "drg_user",
    "password": "drg_password",
    "host": "localhost",
    "port": "5432"
}

def get_connection(retries=5, delay=2):
    """Establish a database connection with retries."""
    for i in range(retries):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            return conn
        except psycopg2.OperationalError as e:
            if i == retries - 1:
                raise e
            time.sleep(delay)

@contextmanager
def get_db_cursor(commit=False):
    """Context manager for database cursor."""
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        yield cur
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def fetch_one(query, params=None):
    with get_db_cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()

def fetch_all(query, params=None):
    with get_db_cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()

def execute_query(query, params=None):
    with get_db_cursor(commit=True) as cur:
        cur.execute(query, params)
