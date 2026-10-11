import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.getenv("TIGER_HOST"),
        port=os.getenv("TIGER_PORT"),
        dbname=os.getenv("TIGER_DB"),
        user=os.getenv("TIGER_USER"),
        password=os.getenv("TIGER_PASSWORD"),
        sslmode="require",
        connect_timeout=15,
    )

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM stocks_real_time")
        print("Connection successful!")
        print("Current records:", cur.fetchone()[0])

    conn.close()

except Exception as error:
    print("Connection failed:", error)