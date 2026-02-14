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

def merge_to_gold(con, gold_path, arrow_table, unique_keys, partition_cols=None):
    if not arrow_table or len(arrow_table) == 0:
        return

    if not os.path.exists(gold_path):
        try:
            write_deltalake(gold_path, arrow_table, mode="overwrite", partition_by=partition_cols)
        except Exception as e:
            print(f"Error creating Gold table {gold_path}: {e}")
    else:
        try:
            dt = DeltaTable(gold_path)
            
            # Encapsulate logic for safety
            unique_keys = unique_keys or []
            partition_cols = partition_cols or []
            
            all_keys = unique_keys + [c for c in partition_cols if c not in unique_keys]
            predicate = " AND ".join([f"s.{k} = t.{k}" for k in all_keys])
            
            (
                dt.merge(
                    source=arrow_table,
                    predicate=predicate,
                    source_alias="s",
                    target_alias="t"
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
            
            # --- Performance Optimization ---
            # Compaction: Merge small files into larger ones
            dt.optimize().execute_compaction()
            
            # Checkpoint: Create a checkpoint file to speed up log reading
            dt.create_checkpoint()

            # Vacuum: Remove old files (retention default is 7 days usually)
            # dt.vacuum(retention_hours=168) 
            
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
            df_prod = con.query("SELECT * FROM dim_product_tmp").arrow().read_all()
            merge_to_gold(con, os.path.join(gold_root, 'dim_product'), df_prod, unique_keys=["product_id"])
    finally:
        con.close()

def process_dim_customer(silver_root, gold_root, scans, updates):
    if not (updates['retail'] or updates['pos']):
        return

    con = init_duckdb()
    try:
        print("Updating dim_customer (Delta - SCD Type 2 Incremental)...")
        gold_path = os.path.join(gold_root, 'dim_customer')
        
        # 1. Get Watermark (Max valid_from in Gold)
        watermark = "1900-01-01"
        if os.path.exists(gold_path):
            try:
                # Optimized Watermark Retrieval using DeltaTable directly
                # Avoids DuckDB delta_scan overhead of parsing all log files
                dt = DeltaTable(gold_path)
                
                # We only need the max valid_from. 
                # Reading the column via PyArrow is much faster than SQL scan for metadata 
                # if files are compacted, and avoids full log re-parsing if we just opened it.
                # Note: If the table is huge, we might want to rely on partition stats if partitioned by date.
                # For Dim Customer, it's not partitioned by date, so we read the column.
                # However, we can also use a SQL query on the DeltaTable object if needed, 
                # but standard arrow reduction is usually fast enough for Dimensions.
                
                # Check if table is empty first
                if dt.version() >= 0:
                     # This pulls the column into memory but it's just one timestamp column
                    max_val = dt.to_pyarrow_table(columns=["valid_from"]).column("valid_from").max().as_py()
                    if max_val:
                        watermark = str(max_val)
            except Exception as e:
                print(f"Warning: Could not get watermark from {gold_path}, doing full load. Error: {e}")

        print(f"SCD2 Watermark: {watermark}")

        # 2. Query New Data from Silver ( > Watermark )
        sources = []
        if updates['retail']: sources.append(f"SELECT customer_id, city, country, order_timestamp, 'Online' as source FROM {scans['retail']} WHERE order_timestamp > '{watermark}'")
        if updates['pos']: sources.append(f"SELECT customer_id, city, country, order_timestamp, 'POS' as source FROM {scans['pos']} WHERE order_timestamp > '{watermark}'")
        
        if not sources:
            print("No new data found via watermark.")
            return

        union_query = " UNION ALL ".join(sources)
        
        # 3. Prepare Staging Data (New Versions)
        # We process the new stream to find internal changes first
        con.execute(f"""
            CREATE OR REPLACE TABLE dim_customer_stage AS
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
            -- Identify changes WITHIN the new batch
            batch_changes AS (
                SELECT 
                    customer_id, city, country, source, order_timestamp as valid_from
                FROM ordered_customers
                WHERE prev_city IS NULL OR city != prev_city OR country != prev_country
            ),
            -- Deduplicate: Keep only the earliest occurrence of a change in the batch per day/timestamp?
            -- Actually, if a user changes city twice in a batch, we capture both.
            batch_dedup AS (
                SELECT * 
                FROM batch_changes
                QUALIFY ROW_NUMBER() OVER(PARTITION BY customer_id, valid_from ORDER BY valid_from) = 1
            )
            SELECT * FROM batch_dedup
        """)
        
        if not os.path.exists(gold_path):
            # First Run: Create table from Stage
            print("First run: Creating dim_customer from Staging...")
            con.execute(f"""
                CREATE OR REPLACE TABLE dim_customer_final AS
                SELECT 
                    CAST((hash(customer_id || valid_from::VARCHAR) & 9223372036854775807) AS BIGINT) as customer_key,
                    customer_id, city, country, source,
                    valid_from,
                    LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from) as valid_to,
                    (LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from) IS NULL) as is_current
                FROM dim_customer_stage
                UNION ALL
                SELECT 0, 'Unknown', 'Unknown', 'Unknown', 'SYSTEM', '1900-01-01'::TIMESTAMP, NULL::TIMESTAMP, true
            """)
            df_cust = con.query("SELECT * FROM dim_customer_final").arrow().read_all()
            write_deltalake(gold_path, df_cust, mode="overwrite")
            return

        # 4. Incremental Merge Logic
        # We need to compare batch_dedup (Staging) with Current Gold.
        # If Staging is truly new (diff city than current gold), we insert.
        # AND we update the old record.
        
        print("SCD2 Merge: Calculating updates and inserts...")
        
        # Pull current Gold records for affected customers
        con.execute(f"""
            CREATE OR REPLACE TABLE current_gold AS
            SELECT customer_key, customer_id, city, country, valid_from, valid_to
            FROM delta_scan('{gold_path}')
            WHERE is_current = true
            AND customer_id IN (SELECT DISTINCT customer_id FROM dim_customer_stage)
        """)
        
        # Determine Logic
        # New rows that are actually changes relative to Gold
        # Or New customers entirely
        con.execute("""
            CREATE OR REPLACE TABLE scd_ops AS
            SELECT 
                s.customer_id, s.city, s.country, s.source, s.valid_from,
                g.customer_key as old_customer_key,
                g.valid_from as old_valid_from,
                
                CASE 
                    WHEN g.customer_id IS NULL THEN 'INSERT_NEW' -- New Customer
                    WHEN s.city != g.city OR s.country != g.country THEN 'UPDATE_INSERT' -- Changed
                    -- Also cover case where we simply have newer data confirming same state? Ignore.
                    ELSE 'IGNORE' 
                END as op_type
            FROM dim_customer_stage s
            LEFT JOIN current_gold g ON s.customer_id = g.customer_id
            -- Filter out earlier dates if any (watermark should prevent, but safe guard)
            WHERE s.valid_from > COALESCE(g.valid_from, '1900-01-01')
        """)
        
        # Prepare Merge Source
        # 1. Updates: Rows to match and Update (Close old)
        # 2. Inserts: Rows to Insert (New version)
        
        # We need to construct a standard Merge Source.
        # Common keys for Merge: `customer_key` (if updating) OR NULL (if inserting).
        
        # Rows to Update (Close current):
        # Need to match on `customer_key`.
        # Set `is_current` = false, `valid_to` = new `valid_from`.
        
        # Rows to Insert (Open new):
        # Key = NULL (force Not Matched).
        # Set `is_current` = true, `valid_from` = new `valid_from`, ...
        
        con.execute(f"""
            CREATE OR REPLACE TABLE merge_source AS
            -- Operation 1: UPDATE existing current rows
            SELECT 
                old_customer_key as merge_key, -- Matches Gold
                customer_id, city, country, source,
                valid_from as new_valid_from, -- This becomes valid_to for old record
                false as new_is_current,
                'UPDATE' as merge_action
            FROM scd_ops
            WHERE op_type = 'UPDATE_INSERT'
            
            UNION ALL
            
            -- Operation 2: INSERT new rows (for both New Customers and New Versions of existing)
            SELECT 
                NULL as merge_key, -- Forces Insert
                customer_id, city, country, source,
                valid_from as new_valid_from, -- This is valid_from for new record
                true as new_is_current,
                'INSERT' as merge_action
            FROM scd_ops
            WHERE op_type IN ('INSERT_NEW', 'UPDATE_INSERT')
        """)
        
        arrow_source = con.query("""
            SELECT 
                merge_key, 
                customer_id, city, country, source, 
                new_valid_from, new_is_current, merge_action, 
                -- Generate new key for inserts
                CAST((hash(customer_id || new_valid_from::VARCHAR) & 9223372036854775807) AS BIGINT) as generated_key
            FROM merge_source
        """).arrow().read_all()
        
        if len(arrow_source) > 0:
            dt = DeltaTable(gold_path)
            (
                dt.merge(
                    source=arrow_source,
                    predicate="t.customer_key = s.merge_key",
                    source_alias="s",
                    target_alias="t"
                )
                .when_matched_update(
                    predicate="s.merge_action = 'UPDATE'",
                    updates={
                        "is_current": "s.new_is_current",
                        "valid_to": "s.new_valid_from"
                    }
                )
                .when_not_matched_insert(
                    predicate="s.merge_action = 'INSERT'",
                    updates={
                        "customer_key": "s.generated_key",
                        "customer_id": "s.customer_id",
                        "city": "s.city",
                        "country": "s.country",
                        "source": "s.source",
                        "valid_from": "s.new_valid_from",
                        "valid_to": "NULL",
                        "is_current": "s.new_is_current"
                    }
                )
                .execute()
            )
            
            # Optimization for Dim Customer
            dt.optimize().execute_compaction()
            dt.create_checkpoint()
            
            print("SCD2 Merge complete.")
        else:
            print("No SCD2 changes to merge.")

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
        df_date = con.query("SELECT * FROM dim_date_tmp").arrow().read_all()
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
            
            df_sales = con.query("SELECT * FROM fact_sales_tmp").arrow().read_all()
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
        df_inv = con.query("SELECT * FROM fact_inventory_tmp").arrow().read_all()
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
        df_ship = con.query("SELECT * FROM fact_shipments_tmp").arrow().read_all()
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
        df_kpi = con.query("SELECT * FROM kpi_summary_tmp").arrow().read_all()
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
