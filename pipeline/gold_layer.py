import duckdb
import os
import sys
import concurrent.futures
import time

# Add project root to sys.path to import pipeline.utils
sys.path.append(os.getcwd())
from pipeline.utils import get_incremental_scan_query, save_checkpoints, load_checkpoints
from deltalake import DeltaTable, write_deltalake

def init_duckdb():
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    return con

def merge_to_gold(con, gold_path, df, unique_keys, partition_cols=None):
    if df.empty:
        return

    if not os.path.exists(gold_path):
        try:
            write_deltalake(gold_path, df, mode="overwrite", partition_by=partition_cols)
        except Exception as e:
            print(f"Error creating Gold table {gold_path}: {e}")
    else:
        try:
            dt = DeltaTable(gold_path)
            predicate = " AND ".join([f"s.{k} = t.{k}" for k in unique_keys])
            (
                dt.merge(
                    source=df,
                    predicate=predicate,
                    source_alias="s",
                    target_alias="t"
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
        except Exception as e:
            print(f"Error merging into Gold table {gold_path}: {e}")

# --- Worker Functions (Each gets its own connection) ---

def process_dim_product(silver_root, gold_root, scans, updates):
    if not (updates['retail'] or updates['pos'] or updates['warehouse']):
        return

    con = init_duckdb()
    try:
        print("Updating dim_product (Delta)...")
        sources = []
        if updates['retail']: sources.append(f"SELECT DISTINCT product_id, product_description, 'ERP' as source_system FROM {scans['retail']}")
        if updates['pos']: sources.append(f"SELECT DISTINCT product_id, product_description, 'POS' as source_system FROM {scans['pos']}")
        if updates['warehouse']: sources.append(f"SELECT DISTINCT product_id, product_name as product_description, 'WMS' as source_system FROM {scans['warehouse']}")
        
        if sources:
            union_query = " UNION ".join(sources)
            con.execute(f"""
                CREATE OR REPLACE TABLE dim_product_tmp AS
                SELECT 
                    CAST((hash(product_id) & 9223372036854775807) AS BIGINT) as product_key,
                    product_id,
                    product_description,
                    source_system
                FROM (
                    SELECT product_id, product_description, source_system
                    FROM (
                        {union_query}
                    )
                    QUALIFY ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY source_system DESC) = 1
                )
            """)
            df_prod = con.query("SELECT * FROM dim_product_tmp").to_df()
            merge_to_gold(con, os.path.join(gold_root, 'dim_product'), df_prod, unique_keys=["product_id"])
    finally:
        con.close()

def process_dim_customer(silver_root, gold_root, scans, updates):
    if not (updates['retail'] or updates['pos']):
        return

    con = init_duckdb()
    try:
        print("Updating dim_customer (Delta - SCD Type 2)...")
        sources = []
        if updates['retail']: sources.append(f"SELECT customer_id, city, country, order_timestamp, 'Online' as source FROM {scans['retail']}")
        if updates['pos']: sources.append(f"SELECT customer_id, city, country, order_timestamp, 'POS' as source FROM {scans['pos']}")
        
        if sources:
            union_query = " UNION ALL ".join(sources)
            con.execute(f"""
                CREATE OR REPLACE TABLE dim_customer_tmp AS
                WITH raw_customers AS (
                    {union_query}
                ),
                valid_customers AS (
                    SELECT * FROM raw_customers 
                    WHERE customer_id IS NOT NULL AND customer_id != 'Unknown'
                ),
                ordered_customers AS (
                    SELECT 
                        customer_id, city, country, order_timestamp, source,
                        LAG(city) OVER (PARTITION BY customer_id ORDER BY order_timestamp) as prev_city,
                        LAG(country) OVER (PARTITION BY customer_id ORDER BY order_timestamp) as prev_country
                    FROM valid_customers
                ),
                version_starts AS (
                    SELECT 
                        customer_id, city, country, source, order_timestamp as valid_from
                    FROM ordered_customers
                    WHERE prev_city IS NULL OR city != prev_city OR country != prev_country
                ),
                versions AS (
                    SELECT 
                        CAST((hash(customer_id || valid_from::VARCHAR) & 9223372036854775807) AS BIGINT) as customer_key,
                        customer_id, city, country, source,
                        valid_from,
                        LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from) as valid_to,
                        (LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from) IS NULL) as is_current
                    FROM version_starts
                )
                SELECT * FROM versions
                UNION ALL
                SELECT 
                    0 as customer_key, 'Unknown', 'Unknown', 'Unknown', 'SYSTEM', '1900-01-01'::TIMESTAMP, NULL::TIMESTAMP, true
            """)
            df_cust = con.query("SELECT * FROM dim_customer_tmp").to_df()
            write_deltalake(os.path.join(gold_root, 'dim_customer'), df_cust, mode="overwrite")
    finally:
        con.close()

def process_dim_date(silver_root, gold_root, scans, updates):
    if os.path.exists(os.path.join(gold_root, 'dim_date')):
        return

    con = init_duckdb()
    try:
        print("Updating dim_date (Delta)...")
        con.execute(f"""
            CREATE OR REPLACE TABLE dim_date_tmp AS
            WITH date_spine AS (
                SELECT DISTINCT order_timestamp::DATE as full_date FROM {scans['retail']}
                UNION
                SELECT DISTINCT order_timestamp::DATE as full_date FROM {scans['pos']}
            )
            SELECT 
                (YEAR(full_date) * 10000 + MONTH(full_date) * 100 + DAY(full_date))::INTEGER as date_key,
                full_date,
                DAY(full_date) as day,
                MONTH(full_date) as month,
                YEAR(full_date) as year,
                (DAYOFWEEK(full_date) = 0) as is_weekend
            FROM date_spine;
        """)
        df_date = con.query("SELECT * FROM dim_date_tmp").to_df()
        write_deltalake(os.path.join(gold_root, 'dim_date'), df_date, mode="overwrite")
    finally:
        con.close()

def process_fact_sales(silver_root, gold_root, scans, updates):
    if not (updates['retail'] or updates['pos'] or updates['ship']):
        return

    con = init_duckdb()
    try:
        print("Updating fact_sales (Delta)...")
        sales_sources = []
        if updates['retail']: sales_sources.append(f"SELECT * FROM {scans['retail']}")
        if updates['pos']: sales_sources.append(f"SELECT * FROM {scans['pos']}")
        
        if sales_sources:
            union_sales = " UNION ALL BY NAME ".join(sales_sources)
            dim_cust_scan = f"delta_scan('{os.path.join(gold_root, 'dim_customer')}')"
            dim_prod_scan = f"delta_scan('{os.path.join(gold_root, 'dim_product')}')"
            ship_scan = f"delta_scan('{os.path.join(silver_root, 'shipments')}')" # Shipments are shared lookup

            con.execute(f"""
                CREATE OR REPLACE TABLE fact_sales_tmp AS
                WITH combined_sales AS (
                    SELECT 
                        REGEXP_REPLACE(invoice_id, '^C', '', 'i') as invoice_id_new,
                        * EXCLUDE (invoice_id)
                    FROM (
                        {union_sales}
                    )
                ),
                shipment_lookup AS (
                    SELECT 
                        REGEXP_REPLACE(invoice_id, '^C', '', 'i') as invoice_id_new, 
                        city 
                    FROM {ship_scan}
                )
                SELECT 
                    MD5(s.invoice_id_new || s.product_id || quantity::VARCHAR || order_timestamp::VARCHAR) as sales_key,
                    s.invoice_id_new as invoice_id,
                    c.customer_key,
                    p.product_key,
                    (YEAR(s.order_timestamp) * 10000 + MONTH(s.order_timestamp) * 100 + DAY(s.order_timestamp))::INTEGER as date_key,
                    quantity,
                    unit_price,
                    cost_price,
                    total_amount,
                    source_channel,
                    is_cancelled,
                    COALESCE(NULLIF(s.city, 'Unknown'), sh.city, 'Unknown') as city,
                    s.year,
                    s.month,
                    s.day
                FROM combined_sales s
                LEFT JOIN shipment_lookup sh ON s.invoice_id_new = sh.invoice_id_new
                JOIN {dim_cust_scan} c ON s.customer_id = c.customer_id 
                     AND s.order_timestamp >= c.valid_from 
                     AND (s.order_timestamp < c.valid_to OR c.valid_to IS NULL)
                JOIN {dim_prod_scan} p ON s.product_id = p.product_id
            """)
            
            df_sales = con.query("SELECT * FROM fact_sales_tmp").to_df()
            merge_to_gold(con, os.path.join(gold_root, 'fact_sales'), df_sales, ["sales_key"], ["year", "month", "day"])
    finally:
        con.close()

def process_fact_inventory(silver_root, gold_root, scans, updates):
    if not updates['warehouse']:
        return

    con = init_duckdb()
    try:
        print("Updating fact_inventory (Delta)...")
        dim_prod_scan = f"delta_scan('{os.path.join(gold_root, 'dim_product')}')"
        scan_warehouse = scans['warehouse']

        con.execute(f"""
            CREATE OR REPLACE TABLE fact_inventory_tmp AS
            SELECT 
                log_id,
                p.product_key,
                (YEAR(w.log_timestamp) * 10000 + MONTH(w.log_timestamp) * 100 + DAY(w.log_timestamp))::INTEGER as date_key,
                movement_type,
                qty_change,
                warehouse_id,
                supplier,
                weight_kg,
                w.year,
                w.month,
                w.day
            FROM {scan_warehouse} w
            JOIN {dim_prod_scan} p ON w.product_id = p.product_id
        """)
        df_inv = con.query("SELECT * FROM fact_inventory_tmp").to_df()
        merge_to_gold(con, os.path.join(gold_root, 'fact_inventory'), df_inv, ["log_id"], ["year", "month", "day"])
    finally:
        con.close()

def process_fact_shipments(silver_root, gold_root, scans, updates):
    if not updates['ship']:
        return

    con = init_duckdb()
    try:
        print("Updating fact_shipments (Delta)...")
        fact_sales_scan = f"delta_scan('{os.path.join(gold_root, 'fact_sales')}')"
        dim_cust_scan = f"delta_scan('{os.path.join(gold_root, 'dim_customer')}')"
        scan_ship = scans['ship']

        # Ensure source table exists (fallback if scan is empty/None is handled by update check)
        
        con.execute(f"""
            CREATE OR REPLACE TABLE fact_shipments_tmp AS
            WITH unknown_cust AS (
                SELECT customer_key FROM {dim_cust_scan} WHERE customer_id = 'Unknown'
            )
            SELECT 
                REGEXP_REPLACE(sh.invoice_id, '^C', '', 'i') as invoice_id,
                COALESCE(ANY_VALUE(s.customer_key), (SELECT customer_key FROM unknown_cust)) as customer_key,
                (YEAR(TRY_CAST(sh.delivery_timestamp AS TIMESTAMP)) * 10000 + 
                 MONTH(TRY_CAST(sh.delivery_timestamp AS TIMESTAMP)) * 100 + 
                 DAY(TRY_CAST(sh.delivery_timestamp AS TIMESTAMP)))::INTEGER as date_key,
                TRY_CAST(sh.ship_timestamp AS TIMESTAMP) as ship_timestamp,
                TRY_CAST(sh.delivery_timestamp AS TIMESTAMP) as delivery_timestamp,
                datediff('day', TRY_CAST(sh.ship_timestamp AS TIMESTAMP), TRY_CAST(sh.delivery_timestamp AS TIMESTAMP)) as delivery_days,
                sh.city,
                sh.country,
                sh.year,
                sh.month,
                sh.day
            FROM {scan_ship} sh
            LEFT JOIN {fact_sales_scan} s ON REGEXP_REPLACE(sh.invoice_id, '^C', '', 'i') = s.invoice_id
            GROUP BY ALL
        """)
        df_ship = con.query("SELECT * FROM fact_shipments_tmp").to_df()
        merge_to_gold(con, os.path.join(gold_root, 'fact_shipments'), df_ship, ["invoice_id"], ["year", "month", "day"])
    finally:
        con.close()

def process_kpi_summary(silver_root, gold_root, scans, updates):
    # Always update if sales updated
    if not (updates['retail'] or updates['pos']):
        return
        
    con = init_duckdb()
    try:
        print("Updating kpi_summary (Delta)...")
        con.execute(f"""
            CREATE OR REPLACE TABLE kpi_summary_tmp AS
            SELECT 
                SUM(total_amount) as total_revenue,
                COUNT(DISTINCT invoice_id) as total_orders,
                COUNT(DISTINCT customer_key) as total_customers
            FROM delta_scan('{os.path.join(gold_root, 'fact_sales')}')
        """)
        df_kpi = con.query("SELECT * FROM kpi_summary_tmp").to_df()
        write_deltalake(os.path.join(gold_root, 'kpi_summary'), df_kpi, mode="overwrite")
    finally:
        con.close()

def run_gold_transformation(silver_root, gold_root):
    print(f"--- ELT: Transforming Silver to Gold (Unified Star Schema) to Delta ---")
    start_time = time.time()
    
    os.makedirs(gold_root, exist_ok=True)
    
    # Check updates
    online_retail = os.path.join(silver_root, 'online_retail')
    pos_billing = os.path.join(silver_root, 'pos_billing')
    warehouse_logs = os.path.join(silver_root, 'warehouse_logs')
    shipments = os.path.join(silver_root, 'shipments')

    scan_retail, v_retail, u_retail = get_incremental_scan_query(online_retail, "gold_dim_inputs_retail")
    scan_pos, v_pos, u_pos = get_incremental_scan_query(pos_billing, "gold_dim_inputs_pos")
    scan_warehouse, v_warehouse, u_warehouse = get_incremental_scan_query(warehouse_logs, "gold_dim_inputs_warehouse")
    scan_ship, v_ship, u_ship = get_incremental_scan_query(shipments, "gold_dim_inputs_ship")
    
    updates = {
        'retail': u_retail, 'pos': u_pos, 'warehouse': u_warehouse, 'ship': u_ship
    }
    scans = {
        'retail': scan_retail, 'pos': scan_pos, 'warehouse': scan_warehouse, 'ship': scan_ship
    }
    
    if not any(updates.values()):
        print("No updates in any Silver table. Skipping Gold processing.")
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        # Phase 1: Dimensions
        print("--- Gold Phase 1: Dimensions ---")
        futures_p1 = [
            executor.submit(process_dim_product, silver_root, gold_root, scans, updates),
            executor.submit(process_dim_customer, silver_root, gold_root, scans, updates),
            executor.submit(process_dim_date, silver_root, gold_root, scans, updates)
        ]
        concurrent.futures.wait(futures_p1)
        for f in futures_p1: 
            if f.exception(): print(f"Dimensions failed: {f.exception()}")

        # Phase 2: Independent Facts
        print("--- Gold Phase 2: Independent Facts ---")
        futures_p2 = [
            executor.submit(process_fact_sales, silver_root, gold_root, scans, updates),
            executor.submit(process_fact_inventory, silver_root, gold_root, scans, updates)
        ]
        concurrent.futures.wait(futures_p2)
        for f in futures_p2: 
            if f.exception(): print(f"Facts P1 failed: {f.exception()}")

        # Phase 3: Dependent Facts
        print("--- Gold Phase 3: Dependent Facts & Aggregates ---")
        futures_p3 = [
            executor.submit(process_fact_shipments, silver_root, gold_root, scans, updates),
            executor.submit(process_kpi_summary, silver_root, gold_root, scans, updates)
        ]
        concurrent.futures.wait(futures_p3)
        for f in futures_p3: 
            if f.exception(): print(f"Facts P2 failed: {f.exception()}")

    # Save Checkpoints
    checkpoints = load_checkpoints()
    if u_retail: checkpoints["gold_dim_inputs_retail"] = v_retail
    if u_pos: checkpoints["gold_dim_inputs_pos"] = v_pos
    if u_warehouse: checkpoints["gold_dim_inputs_warehouse"] = v_warehouse
    if u_ship: checkpoints["gold_dim_inputs_ship"] = v_ship
    save_checkpoints(checkpoints)

    print(f"Successfully updated Gold layer in {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    SILVER_ROOT = 'medallion/silver'
    GOLD_ROOT = 'medallion/gold'
    try:
        run_gold_transformation(SILVER_ROOT, GOLD_ROOT)
    except Exception as e:
        print(f"Gold Layer Failed: {e}")
