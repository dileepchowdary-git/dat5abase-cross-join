# sync_pg_ch.py
import psycopg2
from psycopg2.extras import RealDictCursor
from clickhouse_driver import Client as CHClient
import datetime
from datetime import timedelta

# Import credentials and tables from db_config
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
# Optimized Account Status Logic
# ------------------------------
def get_account_status_optimized(lad, onboard_status, ob_date, first_case_date):
    """
    Calculate account status based on business rules using pre-fetched data
    """
    # Check onboarding status first
    if onboard_status == 'Yet To Onboard':
        return 'Yet To Onboard'
    
    # If LAD is None or invalid, return Still Born
    if lad is None:
        return "Still Born/Not started"
    
    # Convert LAD to date if it's datetime
    if isinstance(lad, datetime.datetime):
        lad = lad.date()
    elif isinstance(lad, str):
        try:
            lad = datetime.datetime.strptime(lad, '%Y-%m-%d').date()
        except:
            return "Still Born/Not started"
    
    today = datetime.date.today()
    
    # Active: LAD within last 7 days
    if lad >= (today - timedelta(days=7)):
        return "Active"
    
    # In-Active: LAD between 7-30 days ago
    if (lad < (today - timedelta(days=7))) and (lad >= (today - timedelta(days=30))):
        return "In-Active"
    
    # Churned: LAD more than 30 days ago
    if lad < (today - timedelta(days=30)):
        # Convert dates if needed
        if isinstance(ob_date, datetime.datetime):
            ob_date = ob_date.date()
        if isinstance(first_case_date, datetime.datetime):
            first_case_date = first_case_date.date()
        
        # Determine base date (max of onboard_date and first_case_date)
        candidates = [d for d in [ob_date, first_case_date] if d is not None]
        if candidates:
            base_date = max(candidates)
            diff_days = (lad - base_date).days
            if diff_days < 30:
                return "Incubation Churn"
            else:
                return "Post D30 Churn"
        else:
            return "Churned"
    
    return "Unknown"

# ------------------------------
# Optimized Sync function
# ------------------------------
def sync_pg_to_ch(pg_conn, ch_client, table_name):
    print(f"[INFO] Syncing table: {table_name} from PostgreSQL → ClickHouse")
    
    # Step 1: Get ClickHouse table columns
    ch_columns_info = ch_client.execute(f"DESCRIBE TABLE {table_name}")
    ch_columns = [col[0] for col in ch_columns_info]  # extract column names
    
    with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
        if table_name == "client_group":
            print("[INFO] Processing client_group with enhanced account status...")
            
            # Enhanced query to get comprehensive account status
            cur.execute("""
                SELECT cg.*,
                       CASE
                           WHEN l.existing_client IS NULL
                               AND l.client_fk IS NOT NULL
                               AND l.stage <> 'Onboarded'
                           THEN 'Yet To Onboard'
                           ELSE 'Onboarded'
                       END AS onboard_status
                FROM public.client_group cg
                LEFT JOIN public.lead l
                  ON cg.client_fk = l.client_fk
            """)
            
            rows = cur.fetchall()
            print(f"[INFO] Found {len(rows)} client_group records to process")
            
            if not rows:
                print("[WARNING] No client_group data found")
                return
            
            # Get all client_fks for batch processing
            client_fks = [row['client_fk'] for row in rows]
            print(f"[INFO] Getting LAD data for {len(client_fks)} clients...")
            
            # Batch query for LAD data from ClickHouse
            try:
                if client_fks:
                    # Create IN clause for batch query
                    client_fks_str = ','.join(map(str, client_fks))
                    
                    # Batch query for LAD
                    lad_query = f"""
                    SELECT 
                        client_fk,
                        MAX(created_at) as lad
                    FROM transform.Studies
                    WHERE client_fk IN ({client_fks_str}) AND status = 'COMPLETED'
                    GROUP BY client_fk
                    """
                    lad_results = ch_client.execute(lad_query)
                    lad_dict = {row[0]: row[1] for row in lad_results}
                    
                    # Batch query for onboard dates
                    onboard_query = f"""
                    SELECT id, onboarded_at 
                    FROM transform.Clients 
                    WHERE id IN ({client_fks_str})
                    """
                    onboard_results = ch_client.execute(onboard_query)
                    onboard_dict = {row[0]: row[1] for row in onboard_results}
                    
                    # Batch query for first case dates
                    first_case_query = f"""
                    SELECT 
                        client_fk,
                        MIN(created_at) as first_case_date
                    FROM transform.Studies 
                    WHERE client_fk IN ({client_fks_str}) AND status = 'COMPLETED'
                    GROUP BY client_fk
                    """
                    first_case_results = ch_client.execute(first_case_query)
                    first_case_dict = {row[0]: row[1] for row in first_case_results}
                    
                    print("[INFO] ClickHouse batch queries completed successfully")
                    
                else:
                    lad_dict = {}
                    onboard_dict = {}
                    first_case_dict = {}
                    
            except Exception as e:
                print(f"[ERROR] Failed to get ClickHouse data: {e}")
                lad_dict = {}
                onboard_dict = {}
                first_case_dict = {}
            
            # Process each row with pre-fetched data
            enhanced_rows = []
            for i, row in enumerate(rows):
                if i % 100 == 0:  # Progress indicator
                    print(f"[INFO] Processing client {i+1}/{len(rows)}")
                
                client_fk = row['client_fk']
                onboard_status = row['onboard_status']
                
                lad = lad_dict.get(client_fk)
                ob_date = onboard_dict.get(client_fk)
                first_case_date = first_case_dict.get(client_fk)
                
                # Calculate complete account status using pre-fetched data
                account_status = get_account_status_optimized(
                    lad, onboard_status, ob_date, first_case_date
                )
                
                # Update row with complete account status
                row_dict = dict(row)
                row_dict['account_status'] = account_status
                enhanced_rows.append(row_dict)
            
            rows = enhanced_rows
            print(f"[INFO] Completed processing all {len(rows)} records")
            
        else:
            # Generic case for other tables
            cur.execute(f"SELECT * FROM public.{table_name}")
            rows = cur.fetchall()
    
    if not rows:
        print(f"[WARNING] No data found in PostgreSQL table {table_name}")
        return
    
    print(f"[INFO] Preparing {len(rows)} rows for ClickHouse insertion...")
    
    # Step 2: Keep only columns that exist in ClickHouse
    filtered_rows = []
    for row in rows:
        if isinstance(row, dict):
            filtered_row = {col: row[col] for col in ch_columns if col in row}
        else:
            filtered_row = {col: row[col] for col in ch_columns if col in row}
        filtered_rows.append(convert_row_for_ch(filtered_row, filtered_row.keys()))
    
    # Step 3: Truncate and insert
    print(f"[INFO] Truncating and inserting data into ClickHouse table {table_name}...")
    ch_client.execute(f"TRUNCATE TABLE {table_name}")
    ch_client.execute(
        f"INSERT INTO {table_name} ({', '.join(ch_columns)}) VALUES",
        filtered_rows
    )
    
    print(f"[SUCCESS] Table {table_name} synced successfully with complete account status!")

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
        for table in TABLES:
            sync_pg_to_ch(pg_conn, ch_client, table)
        print("[INFO] Sync completed successfully.")
    finally:
        pg_conn.close()
        ch_client.disconnect()

if __name__ == "__main__":
    main()