import duckdb
import os

def process_live_sales(con, bronze_path, silver_path):
    print(f"--- ELT: Processing Live Sales Data (DuckDB) ---")
    
    # Ensure silver path parent exists
    os.makedirs(os.path.dirname(silver_path), exist_ok=True)
    
    # SQL Transformation: 
    # 1. Cast types
    # 2. Add source_system and is_cancelled
    # 3. Deduplicate using QUALIFY
    query = f"""
    COPY (
        SELECT 
            TRIM(order_id)::VARCHAR as order_id,
            TRIM(store_id)::VARCHAR as store_id,
            TRIM(customer_id)::VARCHAR as customer_id,
            TRIM(customer_city) as customer_city,
            TRIM(region) as region,
            TRIM(UPPER(product_category)) as product_category,
            TRIM(UPPER(channel)) as channel,
            quantity,
            price_per_unit,
            discount,
            TRIM(UPPER(payment_method)) as payment_method,
            holiday_flag,
            order_status,
            total_amount,
            timestamp,
            'pos' as source_system,
            (order_status = 'Cancelled') as is_cancelled,
            year,
            month,
            day
        FROM read_parquet('{bronze_path}/**/*.parquet')
        WHERE order_id IS NOT NULL 
          AND order_id != ''
          AND quantity > 0
          AND price_per_unit > 0
        QUALIFY ROW_NUMBER() OVER(PARTITION BY order_id, product_category, quantity ORDER BY timestamp DESC) = 1
    ) TO '{silver_path}' (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE);
    """
    con.execute(query)
    print(f"Successfully transformed Live Sales to {silver_path}")

def process_online_retail(con, bronze_path, silver_path):
    print(f"--- ELT: Processing Online Retail Data (DuckDB) ---")
    
    os.makedirs(os.path.dirname(silver_path), exist_ok=True)
    
    # SQL Transformation:
    # 1. Rename columns
    # 2. Fill missing customer_id
    # 3. Calculate total_amount
    # 4. Deduplicate (entire row)
    query = f"""
    COPY (
        SELECT 
            TRIM(Invoice)::VARCHAR as invoice_id,
            TRIM(UPPER(StockCode))::VARCHAR as product_id,
            COALESCE(NULLIF(TRIM(UPPER(Description)), ''), 'UNKNOWN') as product_description,
            Quantity as quantity,
            InvoiceDate as order_timestamp,
            Price as unit_price,
            COALESCE(NULLIF(TRIM("Customer ID"), ''), 'Unknown') as customer_id,
            TRIM(Country) as country,
            'erp' as source_system,
            ROUND(Quantity * Price, 2) as total_amount,
            (invoice_id LIKE 'C%') as is_cancelled,
            year,
            month,
            day
        FROM read_parquet('{bronze_path}/**/*.parquet')
        WHERE Invoice IS NOT NULL 
          AND Invoice != ''
          AND StockCode IS NOT NULL 
          AND StockCode != ''
          AND Price > 0
          AND (Quantity > 0 OR (TRIM(Invoice) LIKE 'C%' AND Quantity < 0))
        QUALIFY ROW_NUMBER() OVER(PARTITION BY invoice_id, product_id, quantity, order_timestamp) = 1
    ) TO '{silver_path}' (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE);
    """
    con.execute(query)
    print(f"Successfully transformed Online Retail to {silver_path}")

if __name__ == "__main__":
    BRONZE_ROOT = 'medallion/bronze'
    SILVER_ROOT = 'medallion/silver'
    
    con = duckdb.connect()
    
    try:
        process_live_sales(
            con,
            os.path.join(BRONZE_ROOT, 'live_sales_data'),
            os.path.join(SILVER_ROOT, 'live_sales_data')
        )
        
        process_online_retail(
            con,
            os.path.join(BRONZE_ROOT, 'online_retail_II'),
            os.path.join(SILVER_ROOT, 'online_retail_II')
        )
    finally:
        con.close()
