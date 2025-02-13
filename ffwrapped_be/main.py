import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the database URL from the environment variable
DATABASE_URL = os.getenv("RAILWAY_DB_URL")


def test_db_connection():
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(DATABASE_URL)
        cursor = connection.cursor()

        # Execute a simple query
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()

        print(f"Connected to the database. PostgreSQL version: {db_version}")

        # Close the cursor and connection
        cursor.close()
        connection.close()
    except Exception as error:
        print(f"Error connecting to the database: {error}")


if __name__ == "__main__":
    test_db_connection()
