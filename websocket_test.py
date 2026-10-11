
import os
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from twelvedata import TDClient

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("TIGER_HOST"),
    port=os.getenv("TIGER_PORT"),
    dbname=os.getenv("TIGER_DB"),
    user=os.getenv("TIGER_USER"),
    password=os.getenv("TIGER_PASSWORD"),
    sslmode="require",
)

class WebsocketPipeline:
    def __init__(self, conn):
        self.conn = conn
        self.batch = []

    def save_batch(self):
        if not self.batch:
            return

        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO stocks_real_time
                (time, symbol, price, day_volume)
                VALUES %s
                """,
                self.batch,
            )

        self.conn.commit()
        print(f"Successfully saved {len(self.batch)} records!")
        self.batch.clear()

    def on_event(self, event):
        if event.get("event") == "subscribe-status":
            print("Subscription status:", event)
            return

        if event.get("event") != "price":
            return

        try:
            row = (
                datetime.fromtimestamp(
                    int(event["timestamp"]), timezone.utc
                ),
                event["symbol"],
                float(event["price"]),
                int(event["day_volume"])
                if event.get("day_volume") is not None
                else None,
            )

            self.batch.append(row)
            print(f"Received {row[1]}: {row[2]}")

            if len(self.batch) >= 5:
                self.save_batch()

        except (KeyError, TypeError, ValueError) as error:
            print("Invalid price event:", error)

pipeline = WebsocketPipeline(conn)

try:
    client = TDClient(
        apikey=os.getenv("TWELVE_DATA_API_KEY")
    )

    ws = client.websocket(on_event=pipeline.on_event)
    ws.subscribe(["BTC/USD", "ETH/USD"])
    ws.connect()

    print("Pipeline running. Press Ctrl+C to stop.")

    while True:
        ws.heartbeat()
        time.sleep(10)

except KeyboardInterrupt:
    print("Stopping pipeline...")

finally:
    try:
        pipeline.save_batch()
    finally:
        conn.close()
        print("Database connection closed.")