import psycopg2
from psycopg2.extras import RealDictCursor
from clickhouse_driver import Client as CHClient
import time
import datetime

# Import credentials and tables
from db_config import PG_CONFIG, CLICKHOUSE_CONFIG, TABLES

# ------------------------------
# Helper function to convert rows
# ------------------------------
def convert_row_for_ch(row, columns):
    """
    Converts a PostgreSQL row to ClickHouse-compatible values.
    Keep datetime objects as-is for DateTime columns.
    """
    row_values = []
    for col in columns:
        val = row[col]
        if val is None:
            row_values.append(None)
        elif isinstance(val, (int, float, str)):
            row_values.append(val)
        elif isinstance(val, (datetime.date, datetime.datetime)):
            row_values.append(val)
        else:
            row_values.append(str(val))
    return row_values

# ------------------------------
# Sync function
# ------------------------------
def sync_pg_to_ch(pg_conn, ch_client, table_name):
    print(f"[INFO] Syncing table: {table_name} from PostgreSQL → ClickHouse")

    with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"SELECT * FROM public.{table_name}")
        rows = cur.fetchall()

    if not rows:
        print(f"[WARNING] No data found in PostgreSQL table {table_name}")
        return

    columns = rows[0].keys()
    values = [convert_row_for_ch(row, columns) for row in rows]

    ch_client.execute(f"TRUNCATE TABLE {table_name}")
    ch_client.execute(
        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES",
        values
    )

    print(f"[SUCCESS] Table {table_name} synced successfully!")

# ------------------------------
# Main function
# ------------------------------
def main():
    pg_conn = psycopg2.connect(
        host=PG_CONFIG['host'],
        port=PG_CONFIG['port'],
        database=PG_CONFIG['database'],
        user=PG_CONFIG['user'],
        password=PG_CONFIG['password']
    )

    ch_client = CHClient(
        host=CLICKHOUSE_CONFIG['host'],
        port=CLICKHOUSE_CONFIG['port'],
        user=CLICKHOUSE_CONFIG['user'],
        password=CLICKHOUSE_CONFIG['password'],
        database=CLICKHOUSE_CONFIG['database']
    )

    try:
        while True:
            for table in TABLES:
                sync_pg_to_ch(pg_conn, ch_client, table)
            print("[INFO] Waiting 1 hour for next sync...")
            time.sleep(3600)

    except KeyboardInterrupt:
        print("[INFO] Stopping sync...")

    finally:
        pg_conn.close()
        ch_client.disconnect()

if __name__ == "__main__":
    main()
