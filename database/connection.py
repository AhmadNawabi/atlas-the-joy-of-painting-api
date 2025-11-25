import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

# Load enviroment variables 
load_dotenv()

# Database connection details
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "joy_of_painting")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "Joycoding2025")
DB_PORT = os.getenv("DB_PORT", "5432")


def get_connection():
    """
    Return a new databse connection using psycopg2
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT,
            cursor_factory=RealDictCursor
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None
    
if __name__ == "__main__":
    conn = get_connection()
    if conn:
        print("Connection established successfully.")
        conn.close()
    else:
        print("Failed to establish connection.")