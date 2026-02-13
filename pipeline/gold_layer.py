import duckdb
import os
import shutil
from deltalake import DeltaTable, write_deltalake

def init_duckdb():
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    return con

def merge_to_gold(con, gold_path, df, unique_keys, partition_cols=None):
    """
    Merges data into Gold Delta table.
    """
    if df.empty:
        print("No data to merge.")
        return

    if not os.path.exists(gold_path):
        print(f"Creating new Gold table at {gold_path}")
        try:
            write_deltalake(gold_path, df, mode="overwrite", partition_by=partition_cols)
        except Exception as e:
            print(f"Error creating Gold table {gold_path}: {e}")
    else:
        print(f"Merging into existing Gold table at {gold_path}")
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
            # Fallback if complex merge fails? No, better to fail and log.

def run_gold_transformation(con, silver_root, gold_root):
    print(f"--- ELT: Transforming Silver to Gold (Unified Star Schema) to Delta ---")
    
    os.makedirs(gold_root, exist_ok=True)
    
    # Paths to Silver Delta tables
    online_retail = os.path.join(silver_root, 'online_retail')
    pos_billing = os.path.join(silver_root, 'pos_billing')
    warehouse_logs = os.path.join(silver_root, 'warehouse_logs')
    shipments = os.path.join(silver_root, 'shipments')

    # 1. dim_product (Unified)
    # We re-scan Silver because Products can be updated/added anytime.
    # Ideally we should only scan changed files, but for Dimension tables, full scan of distincts is relatively fast.
    print("Updating dim_product (Delta)...")
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
                SELECT DISTINCT product_id, product_description, 'ERP' as source_system FROM delta_scan('{online_retail}')
                UNION
                SELECT DISTINCT product_id, product_description, 'POS' as source_system FROM delta_scan('{pos_billing}')
                UNION
                SELECT DISTINCT product_id, product_name as product_description, 'WMS' as source_system FROM delta_scan('{warehouse_logs}')
            )
            QUALIFY ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY source_system DESC) = 1
        )
    """)
    
    df_prod = con.query("SELECT * FROM dim_product_tmp").to_df()
    merge_to_gold(con, os.path.join(gold_root, 'dim_product'), df_prod, unique_keys=["product_id"])

    # 2. dim_customer (SCD Type 2)
    # Similar issue with `customer_key`. Use Hash.
    print("Updating dim_customer (Delta - SCD Type 2)...")
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_customer_tmp AS
        WITH raw_customers AS (
            SELECT customer_id, city, country, order_timestamp, 'Online' as source FROM delta_scan('{online_retail}')
            UNION ALL
            SELECT customer_id, city, country, order_timestamp, 'POS' as source FROM delta_scan('{pos_billing}')
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
    
    # SCD 2 Logic usually requires careful handling.
    # For simplicity in this "Incremental" step, we can Overwrite Dimensions if they are small enough,
    # BUT we need stable keys. We used Hash Keys.
    # So we can Safely Overwrite Dim Customer every time? Or Merge?
    # Overwrite is safer for SCD recalculation from history.
    # Merging SCD types is hard.
    # Let's Overwrite Dimensions (they are small ~4k customers) but use STABLE Hash Keys.
    write_deltalake(os.path.join(gold_root, 'dim_customer'), df_cust, mode="overwrite")

    # 3. dim_date - Static, usually generated once.
    # We can overwrite or ignore.
    print("Updating dim_date (Delta)...")
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_date_tmp AS
        WITH date_spine AS (
            SELECT DISTINCT order_timestamp::DATE as full_date FROM delta_scan('{online_retail}')
            UNION
            SELECT DISTINCT order_timestamp::DATE as full_date FROM delta_scan('{pos_billing}')
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

    # 4. fact_sales (Incremental)
    # We need to only process *recent* sales?
    # Or process all and MERGE to utilize idempotency?
    # Since we have `sales_key` (hash based), we can MERGE.
    print("Updating fact_sales (Delta)...")
    
    con.execute(f"""
        CREATE OR REPLACE TABLE fact_sales_tmp AS
        WITH combined_sales AS (
            SELECT 
                REGEXP_REPLACE(invoice_id, '^C', '', 'i') as invoice_id_new,
                * EXCLUDE (invoice_id)
            FROM (
                SELECT * FROM delta_scan('{online_retail}')
                UNION ALL BY NAME
                SELECT * FROM delta_scan('{pos_billing}')
            )
        ),
        shipment_lookup AS (
            SELECT 
                REGEXP_REPLACE(invoice_id, '^C', '', 'i') as invoice_id_new, 
                city 
            FROM delta_scan('{shipments}')
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
        JOIN dim_customer_tmp c ON s.customer_id = c.customer_id 
             AND s.order_timestamp >= c.valid_from 
             AND (s.order_timestamp < c.valid_to OR c.valid_to IS NULL)
        JOIN dim_product_tmp p ON s.product_id = p.product_id
    """)
    
    df_sales = con.query("SELECT * FROM fact_sales_tmp").to_df()
    merge_keys = ["sales_key"]
    # Partition by year/month/day
    merge_to_gold(con, os.path.join(gold_root, 'fact_sales'), df_sales, merge_keys, ["year", "month", "day"])


    # 5. fact_inventory (Incremental)
    print("Updating fact_inventory (Delta)...")
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
        FROM delta_scan('{warehouse_logs}') w
        JOIN dim_product_tmp p ON w.product_id = p.product_id
    """)
    df_inv = con.query("SELECT * FROM fact_inventory_tmp").to_df()
    # LogID is unique
    merge_to_gold(con, os.path.join(gold_root, 'fact_inventory'), df_inv, ["log_id"], ["year", "month", "day"])
    
    # 6. fact_shipments (Incremental)
    print("Updating fact_shipments (Delta)...")
    con.execute(f"""
        CREATE OR REPLACE TABLE fact_shipments_tmp AS
        WITH unknown_cust AS (
            SELECT customer_key FROM dim_customer_tmp WHERE customer_id = 'Unknown'
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
        FROM delta_scan('{shipments}') sh
        LEFT JOIN fact_sales_tmp s ON REGEXP_REPLACE(sh.invoice_id, '^C', '', 'i') = s.invoice_id
        GROUP BY ALL
    """)
    df_ship = con.query("SELECT * FROM fact_shipments_tmp").to_df()
    merge_to_gold(con, os.path.join(gold_root, 'fact_shipments'), df_ship, ["invoice_id"], ["year", "month", "day"])
    
    # 7. KPI Summary (Always Overwrite / Aggregate)
    print("Updating kpi_summary (Delta)...")
    con.execute(f"""
        CREATE OR REPLACE TABLE kpi_summary_tmp AS
        SELECT 
            SUM(total_amount) as total_revenue,
            COUNT(DISTINCT invoice_id) as total_orders,
            COUNT(DISTINCT customer_key) as total_customers
        FROM fact_sales_tmp
    """)
    df_kpi = con.query("SELECT * FROM kpi_summary_tmp").to_df()
    write_deltalake(os.path.join(gold_root, 'kpi_summary'), df_kpi, mode="overwrite")

    print(f"Successfully updated Gold layer (Unified Star Schema) in Delta Lake format at {gold_root}")

if __name__ == "__main__":
    SILVER_ROOT = 'medallion/silver'
    GOLD_ROOT = 'medallion/gold'
    
    con = init_duckdb()
    try:
        run_gold_transformation(con, SILVER_ROOT, GOLD_ROOT)
    finally:
        con.close()
