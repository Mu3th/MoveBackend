import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

def get_db():
    conn = psycopg2.connect(
        host=os.getenv("DBHOST", "localhost"),
        port=os.getenv("DBPORT", "5432"),
        dbname=os.getenv("DBNAME", "move_app"),
        user=os.getenv("DBUSER", "postgres"),
        password=os.getenv("DBPASSWORD")
    )
    conn.cursor_factory = RealDictCursor
    return conn

