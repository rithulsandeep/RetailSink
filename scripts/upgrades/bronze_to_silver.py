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
            order_id::VARCHAR as order_id,
            store_id::VARCHAR as store_id,
            customer_id::VARCHAR as customer_id,
            customer_city,
            region,
            product_category,
            channel,
            quantity,
            price_per_unit,
            discount,
            payment_method,
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
        QUALIFY ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY timestamp DESC) = 1
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
            Invoice::VARCHAR as invoice_id,
            StockCode::VARCHAR as product_id,
            Description as product_description,
            Quantity as quantity,
            InvoiceDate as order_timestamp,
            Price as unit_price,
            COALESCE("Customer ID"::VARCHAR, 'Unknown') as customer_id,
            Country as country,
            'erp' as source_system,
            ROUND(Quantity * Price, 2) as total_amount,
            (invoice_id LIKE 'C%') as is_cancelled,
            year,
            month,
            day
        FROM read_parquet('{bronze_path}/**/*.parquet')
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
