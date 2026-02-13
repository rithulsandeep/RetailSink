import duckdb
import os
import sys

def run_transformation(con, silver_root, gold_root):
    print(f"--- ELT: Transforming Silver to Gold (Star Schema) ---")
    
    # Ensure gold path exists
    os.makedirs(gold_root, exist_ok=True)
    
    # Register Silver views
    pos_silver = os.path.join(silver_root, 'live_sales_data', '**', '*.parquet')
    erp_silver = os.path.join(silver_root, 'online_retail_II', '**', '*.parquet')
    
    # Check if files exist to avoid DuckDB errors
    import glob
    if not glob.glob(pos_silver, recursive=True) and not glob.glob(erp_silver, recursive=True):
        print("No silver data found. Skipping Gold transformation.")
        return

    # 1. Create dim_product
    print("Creating dim_product...")
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_product AS
        WITH combined_products AS (
            SELECT DISTINCT 
                product_category as product_id, 
                product_category,
                'Category from POS' as product_description
            FROM read_parquet('{pos_silver}')
            UNION
            SELECT DISTINCT 
                product_id,
                'Uncategorized' as product_category,
                product_description
            FROM read_parquet('{erp_silver}')
        )
        SELECT 
            ROW_NUMBER() OVER () as product_key,
            product_id,
            product_category,
            product_description
        FROM combined_products;
    """)
    con.execute(f"COPY dim_product TO '{os.path.join(gold_root, 'dim_product.parquet')}' (FORMAT PARQUET);")

    # 2. Create dim_customer
    print("Creating dim_customer...")
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_customer AS
        WITH combined_customers AS (
            SELECT DISTINCT 
                customer_id,
                customer_city,
                'India' as country
            FROM read_parquet('{pos_silver}')
            UNION
            SELECT DISTINCT 
                customer_id,
                'Unknown' as customer_city,
                country
            FROM read_parquet('{erp_silver}')
        )
        SELECT 
            ROW_NUMBER() OVER () as customer_key,
            customer_id,
            customer_city,
            country
        FROM combined_customers;
    """)
    con.execute(f"COPY dim_customer TO '{os.path.join(gold_root, 'dim_customer.parquet')}' (FORMAT PARQUET);")

    # 3. Create dim_location
    print("Creating dim_location...")
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_location AS
        WITH combined_locations AS (
            SELECT DISTINCT 
                store_id,
                region,
                'India' as country
            FROM read_parquet('{pos_silver}')
            UNION
            SELECT DISTINCT 
                'Online' as store_id,
                'Global' as region,
                country
            FROM read_parquet('{erp_silver}')
        )
        SELECT 
            ROW_NUMBER() OVER () as location_key,
            store_id,
            region,
            country
        FROM combined_locations;
    """)
    con.execute(f"COPY dim_location TO '{os.path.join(gold_root, 'dim_location.parquet')}' (FORMAT PARQUET);")

    # 4. Create dim_date
    print("Creating dim_date...")
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_date AS
        WITH date_spine AS (
            SELECT DISTINCT timestamp::DATE as full_date FROM read_parquet('{pos_silver}')
            UNION
            SELECT DISTINCT order_timestamp::DATE as full_date FROM read_parquet('{erp_silver}')
        )
        SELECT 
            (YEAR(full_date) * 10000 + MONTH(full_date) * 100 + DAY(full_date))::INTEGER as date_key,
            full_date,
            DAY(full_date) as day,
            MONTH(full_date) as month,
            YEAR(full_date) as year,
            (DAYOFWEEK(full_date) = 0) as is_holiday -- Simple Sunday rule
        FROM date_spine;
    """)
    con.execute(f"COPY dim_date TO '{os.path.join(gold_root, 'dim_date.parquet')}' (FORMAT PARQUET);")

    # 5. Create fact_sales
    print("Creating fact_sales...")
    con.execute(f"""
        CREATE OR REPLACE TABLE fact_sales AS
        WITH pos_facts AS (
            SELECT 
                s.order_id,
                c.customer_key,
                p.product_key,
                l.location_key,
                d.date_key,
                s.quantity,
                s.price_per_unit as unit_price,
                (s.quantity * s.price_per_unit * s.discount) as discount_amount,
                s.total_amount,
                s.source_system,
                s.is_cancelled,
                d.year,
                d.month,
                d.day
            FROM read_parquet('{pos_silver}') s
            JOIN dim_customer c ON s.customer_id = c.customer_id
            JOIN dim_product p ON s.product_category = p.product_id
            JOIN dim_location l ON s.store_id = l.store_id AND s.region = l.region
            JOIN dim_date d ON s.timestamp::DATE = d.full_date
        ),
        erp_facts AS (
            SELECT 
                s.invoice_id as order_id,
                c.customer_key,
                p.product_key,
                l.location_key,
                d.date_key,
                s.quantity,
                s.unit_price,
                0.0 as discount_amount,
                s.total_amount,
                s.source_system,
                s.is_cancelled,
                d.year,
                d.month,
                d.day
            FROM read_parquet('{erp_silver}') s
            JOIN dim_customer c ON s.customer_id = c.customer_id
            JOIN dim_product p ON s.product_id = p.product_id
            JOIN dim_location l ON l.store_id = 'Online' AND s.country = l.country
            JOIN dim_date d ON s.order_timestamp::DATE = d.full_date
        )
        SELECT 
            MD5(order_id || source_system || COALESCE(product_key::VARCHAR, '')) as sales_key,
            *
        FROM (SELECT * FROM pos_facts UNION ALL SELECT * FROM erp_facts);
    """)
    
    # Export with partitioning
    fact_sales_path = os.path.join(gold_root, 'fact_sales')
    os.makedirs(fact_sales_path, exist_ok=True)
    con.execute(f"COPY fact_sales TO '{fact_sales_path}' (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE);")
    
    print(f"Successfully created Gold layer tables in {gold_root} (Fact table is partitioned)")

if __name__ == "__main__":
    SILVER_ROOT = 'medallion/silver'
    GOLD_ROOT = 'medallion/gold'
    
    con = duckdb.connect()
    try:
        run_transformation(con, SILVER_ROOT, GOLD_ROOT)
    finally:
        con.close()
