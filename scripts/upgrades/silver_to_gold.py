import duckdb
import os
import glob

def run_gold_transformation(con, silver_root, gold_root):
    print(f"--- ELT: Transforming Silver to Gold (Unified Star Schema) ---")
    
    # Clean Gold directory to prevent stale files from inflating row counts
    if os.path.exists(gold_root):
        import shutil
        print(f"Clearing old Gold layer data in {gold_root}...")
        shutil.rmtree(gold_root)
    os.makedirs(gold_root, exist_ok=True)
    
    # Paths to Silver Parquet files
    online_retail = os.path.join(silver_root, 'online_retail', '**', '*.parquet')
    pos_billing = os.path.join(silver_root, 'pos_billing', '**', '*.parquet')
    warehouse_logs = os.path.join(silver_root, 'warehouse_logs', '**', '*.parquet')

    # 1. dim_product (Truly Unified - unique per product_id)
    print("Creating dim_product...")
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_product AS
        SELECT 
            ROW_NUMBER() OVER () as product_key,
            product_id,
            product_description,
            source_system
        FROM (
            SELECT product_id, product_description, source_system
            FROM (
                SELECT DISTINCT product_id, product_description, 'ERP' as source_system FROM read_parquet('{online_retail}')
                UNION
                SELECT DISTINCT product_id, product_description, 'POS' as source_system FROM read_parquet('{pos_billing}')
                UNION
                SELECT DISTINCT product_id, product_name as product_description, 'WMS' as source_system FROM read_parquet('{warehouse_logs}')
            )
            QUALIFY ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY source_system DESC) = 1
        )
    """)
    con.execute(f"COPY dim_product TO '{os.path.join(gold_root, 'dim_product.parquet')}' (FORMAT PARQUET);")

    # 2. dim_customer (Truly Unified - unique per customer_id)
    print("Creating dim_customer...")
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_customer AS
        SELECT 
            ROW_NUMBER() OVER () as customer_key,
            customer_id,
            source
        FROM (
            SELECT customer_id, source
            FROM (
                SELECT DISTINCT customer_id, 'Online' as source FROM read_parquet('{online_retail}')
                UNION
                SELECT DISTINCT customer_id, 'POS' as source FROM read_parquet('{pos_billing}')
            )
            WHERE customer_id IS NOT NULL AND customer_id != 'Unknown'
            QUALIFY ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY source) = 1
            
            UNION ALL
            -- Ensure 'Unknown' remains in the dimension
            SELECT 'Unknown' as customer_id, 'SYSTEM' as source
        )
    """)
    con.execute(f"COPY dim_customer TO '{os.path.join(gold_root, 'dim_customer.parquet')}' (FORMAT PARQUET);")

    # 3. dim_date
    print("Creating dim_date...")
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_date AS
        WITH date_spine AS (
            SELECT DISTINCT order_timestamp::DATE as full_date FROM read_parquet('{online_retail}')
            UNION
            SELECT DISTINCT order_timestamp::DATE as full_date FROM read_parquet('{pos_billing}')
            UNION
            SELECT DISTINCT log_timestamp::DATE as full_date FROM read_parquet('{warehouse_logs}')
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
    con.execute(f"COPY dim_date TO '{os.path.join(gold_root, 'dim_date.parquet')}' (FORMAT PARQUET);")

    # 4. fact_sales (Unified Online + POS)
    print("Creating fact_sales...")
    con.execute(f"""
        CREATE OR REPLACE TABLE fact_sales AS
        WITH combined_sales AS (
            SELECT * FROM read_parquet('{online_retail}')
            UNION ALL
            SELECT * FROM read_parquet('{pos_billing}')
        )
        SELECT 
            MD5(invoice_id || s.product_id || quantity::VARCHAR || order_timestamp::VARCHAR) as sales_key,
            invoice_id,
            customer_key,
            product_key,
            date_key,
            quantity,
            unit_price,
            cost_price,
            total_amount,
            source_channel,
            is_cancelled,
            s.year,
            s.month,
            s.day
        FROM combined_sales s
        JOIN dim_customer c ON s.customer_id = c.customer_id
        JOIN dim_product p ON s.product_id = p.product_id
        JOIN dim_date d ON s.order_timestamp::DATE = d.full_date
    """)
    
    fact_sales_path = os.path.join(gold_root, 'fact_sales')
    os.makedirs(fact_sales_path, exist_ok=True)
    con.execute(f"COPY fact_sales TO '{fact_sales_path}' (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE);")

    # 5. fact_inventory_movement
    print("Creating fact_inventory_movement...")
    con.execute(f"""
        CREATE OR REPLACE TABLE fact_inventory AS
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
        FROM read_parquet('{warehouse_logs}') w
        JOIN dim_product p ON w.product_id = p.product_id
        JOIN dim_date d ON w.log_timestamp::DATE = d.full_date
    """)
    
    fact_inv_path = os.path.join(gold_root, 'fact_inventory')
    os.makedirs(fact_inv_path, exist_ok=True)
    con.execute(f"COPY fact_inventory TO '{fact_inv_path}' (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE);")
    
    print(f"Successfully created Gold layer (Unified Star Schema) in {gold_root}")

if __name__ == "__main__":
    SILVER_ROOT = 'medallion/silver'
    GOLD_ROOT = 'medallion/gold'
    
    con = duckdb.connect()
    try:
        run_gold_transformation(con, SILVER_ROOT, GOLD_ROOT)
    finally:
        con.close()
