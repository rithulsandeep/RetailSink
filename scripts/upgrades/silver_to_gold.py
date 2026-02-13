import duckdb
import os
import shutil
from deltalake.writer import write_deltalake

def init_duckdb():
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    return con

def run_gold_transformation(con, silver_root, gold_root):
    print(f"--- ELT: Transforming Silver to Gold (Unified Star Schema) to Delta ---")
    
    # In Delta Lake, we don't necessarily rmtree if we use mode="overwrite" in write_deltalake,
    # but for a clean start in this migration, clearing the gold_root is fine.
    if os.path.exists(gold_root):
        print(f"Clearing old Gold layer data in {gold_root}...")
        shutil.rmtree(gold_root)
    os.makedirs(gold_root, exist_ok=True)
    
    # Paths to Silver Delta tables (root directories)
    online_retail = os.path.join(silver_root, 'online_retail')
    pos_billing = os.path.join(silver_root, 'pos_billing')
    warehouse_logs = os.path.join(silver_root, 'warehouse_logs')
    shipments = os.path.join(silver_root, 'shipments')

    # 1. dim_product (Truly Unified - unique per product_id)
    print("Creating dim_product (Delta)...")
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_product_tmp AS
        SELECT 
            ROW_NUMBER() OVER () as product_key,
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
    write_deltalake(os.path.join(gold_root, 'dim_product'), df_prod, mode="overwrite")

    # 2. dim_customer (Truly Unified - unique per customer_id)
    print("Creating dim_customer (Delta)...")
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_customer_tmp AS
        SELECT 
            ROW_NUMBER() OVER () as customer_key,
            customer_id,
            source
        FROM (
            SELECT customer_id, source
            FROM (
                SELECT DISTINCT customer_id, 'Online' as source FROM delta_scan('{online_retail}')
                UNION
                SELECT DISTINCT customer_id, 'POS' as source FROM delta_scan('{pos_billing}')
            )
            WHERE customer_id IS NOT NULL AND customer_id != 'Unknown'
            QUALIFY ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY source) = 1
            
            UNION ALL
            -- Ensure 'Unknown' remains in the dimension
            SELECT 'Unknown' as customer_id, 'SYSTEM' as source
        )
    """)
    df_cust = con.query("SELECT * FROM dim_customer_tmp").to_df()
    write_deltalake(os.path.join(gold_root, 'dim_customer'), df_cust, mode="overwrite")

    # 3. dim_date
    print("Creating dim_date (Delta)...")
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_date_tmp AS
        WITH date_spine AS (
            SELECT DISTINCT order_timestamp::DATE as full_date FROM delta_scan('{online_retail}')
            UNION
            SELECT DISTINCT order_timestamp::DATE as full_date FROM delta_scan('{pos_billing}')
            UNION
            SELECT DISTINCT log_timestamp::DATE as full_date FROM delta_scan('{warehouse_logs}')
            UNION
            SELECT DISTINCT ship_timestamp::DATE as full_date FROM delta_scan('{shipments}')
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

    # 4. fact_sales (Unified Online + POS with City)
    print("Creating fact_sales (Delta)...")
    
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
            customer_key,
            product_key,
            date_key,
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
        JOIN dim_product_tmp p ON s.product_id = p.product_id
        JOIN dim_date_tmp d ON TRY_CAST(s.order_timestamp AS DATE) = d.full_date
    """)
    
    df_sales = con.query("SELECT * FROM fact_sales_tmp").to_df()
    write_deltalake(os.path.join(gold_root, 'fact_sales'), df_sales, mode="overwrite", partition_by=["year", "month", "day"])

    # 5. fact_inventory_movement
    print("Creating fact_inventory (Delta)...")
    con.execute(f"""
        CREATE OR REPLACE TABLE fact_inventory_tmp AS
        SELECT 
            log_id,
            product_key,
            date_key,
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
        JOIN dim_date_tmp d ON w.log_timestamp::DATE = d.full_date
    """)
    df_inv = con.query("SELECT * FROM fact_inventory_tmp").to_df()
    write_deltalake(os.path.join(gold_root, 'fact_inventory'), df_inv, mode="overwrite", partition_by=["year", "month", "day"])
    
    # 6. fact_shipments
    print("Creating fact_shipments (Delta)...")
    con.execute(f"""
        CREATE OR REPLACE TABLE fact_shipments_tmp AS
        SELECT 
            REGEXP_REPLACE(sh.invoice_id, '^C', '', 'i') as invoice_id,
            TRY_CAST(sh.ship_timestamp AS TIMESTAMP) as ship_timestamp,
            TRY_CAST(sh.delivery_timestamp AS TIMESTAMP) as delivery_timestamp,
            datediff('day', TRY_CAST(sh.ship_timestamp AS TIMESTAMP), TRY_CAST(sh.delivery_timestamp AS TIMESTAMP)) as delivery_days,
            sh.city,
            sh.country,
            sh.year,
            sh.month,
            sh.day
        FROM delta_scan('{shipments}') sh
    """)
    df_ship = con.query("SELECT * FROM fact_shipments_tmp").to_df()
    write_deltalake(os.path.join(gold_root, 'fact_shipments'), df_ship, mode="overwrite", partition_by=["year", "month", "day"])
    
    print(f"Successfully created Gold layer (Unified Star Schema) in Delta Lake format at {gold_root}")

if __name__ == "__main__":
    SILVER_ROOT = 'medallion/silver'
    GOLD_ROOT = 'medallion/gold'
    
    con = init_duckdb()
    try:
        run_gold_transformation(con, SILVER_ROOT, GOLD_ROOT)
    finally:
        con.close()
