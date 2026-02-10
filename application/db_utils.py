from dotenv import load_dotenv
import os
import psycopg2

script_dir = os.path.dirname(os.path.abspath(__file__))

def db_connect():
    load_dotenv(os.path.join(script_dir, '../.env'))

    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")

    try:
        conn = psycopg2.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database="sncf"
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None