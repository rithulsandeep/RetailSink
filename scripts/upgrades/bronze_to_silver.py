import duckdb
import os

def process_online_retail(con, bronze_path, silver_path):
    print(f"--- ELT: Normalizing Online Retail (ERP) ---")
    os.makedirs(os.path.dirname(silver_path), exist_ok=True)
    
    query = f"""
    COPY (
        SELECT 
            TRIM(InvoiceNo)::VARCHAR as invoice_id,
            TRIM(UPPER(StockCode))::VARCHAR as product_id,
            COALESCE(NULLIF(TRIM(UPPER(Description)), ''), 'UNKNOWN') as product_description,
            Quantity::INTEGER as quantity,
            InvoiceDate as order_timestamp,
            UnitPrice::DOUBLE as unit_price,
            COALESCE(NULLIF(TRIM(CustomerID::VARCHAR), ''), 'Unknown') as customer_id,
            TRIM(Country) as country,
            'Online' as source_channel,
            ROUND(Quantity::DOUBLE * UnitPrice::DOUBLE, 2) as total_amount,
            Cost_Price::DOUBLE as cost_price,
            (invoice_id LIKE 'C%') as is_cancelled,
            year,
            month,
            day
        FROM read_parquet('{bronze_path}/**/*.parquet')
        WHERE invoice_id IS NOT NULL 
          AND invoice_id != ''
          AND TRY_CAST(UnitPrice AS DOUBLE) > 0
        QUALIFY ROW_NUMBER() OVER(PARTITION BY invoice_id, product_id, quantity, order_timestamp) = 1
    ) TO '{silver_path}' (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE);
    """
    con.execute(query)
    print(f"Successfully normalized Online Retail to {silver_path}")

def process_pos_billing(con, bronze_path, silver_path):
    print(f"--- ELT: Normalizing POS Billing (In-Store) ---")
    os.makedirs(os.path.dirname(silver_path), exist_ok=True)
    
    # Mapping POS synonyms back to standard names
    query = f"""
    COPY (
        SELECT 
            TRIM(BillNo)::VARCHAR as invoice_id,
            TRIM(UPPER(ItemCode))::VARCHAR as product_id,
            COALESCE(NULLIF(TRIM(UPPER(ProductName)), ''), 'UNKNOWN') as product_description,
            TRY_CAST(Qty AS INTEGER) as quantity,
            BillDate as order_timestamp,
            TRY_CAST(Rate AS DOUBLE) as unit_price,
            COALESCE(NULLIF(TRIM(LoyaltyID::VARCHAR), ''), 'Unknown') as customer_id,
            TRIM(Nation) as country,
            'Store' as source_channel,
            ROUND(quantity * unit_price, 2) as total_amount,
            BuyPrice::DOUBLE as cost_price,
            (quantity < 0) as is_cancelled,
            year,
            month,
            day
        FROM read_parquet('{bronze_path}/**/*.parquet')
        WHERE invoice_id IS NOT NULL 
          AND invoice_id != ''
          AND quantity IS NOT NULL
          AND unit_price IS NOT NULL
          AND unit_price > 0
        QUALIFY ROW_NUMBER() OVER(PARTITION BY invoice_id, product_id, quantity, order_timestamp) = 1
    ) TO '{silver_path}' (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE);
    """
    con.execute(query)
    print(f"Successfully normalized POS Billing to {silver_path}")

def process_warehouse(con, bronze_path, silver_path):
    print(f"--- ELT: Normalizing Warehouse Logs ---")
    os.makedirs(os.path.dirname(silver_path), exist_ok=True)
    
    query = f"""
    COPY (
        SELECT 
            LogID as log_id,
            WarehouseCode as warehouse_id,
            TRIM(UPPER(SKU_ID)) as product_id,
            TRIM(UPPER(Item_Name)) as product_name,
            BatchNo as batch_id,
            UPPER(TRIM(MovementType)) as movement_type,
            Quantity_Change::INTEGER as qty_change,
            EventDate as log_timestamp,
            SupplierName as supplier,
            PackageWeight_kg as weight_kg,
            year,
            month,
            day
        FROM read_parquet('{bronze_path}/**/*.parquet')
        WHERE product_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER(PARTITION BY log_id) = 1
    ) TO '{silver_path}' (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE);
    """
    con.execute(query)
    print(f"Successfully normalized Warehouse Logs to {silver_path}")

if __name__ == "__main__":
    BRONZE_ROOT = 'medallion/bronze'
    SILVER_ROOT = 'medallion/silver'
    
    con = duckdb.connect()
    
    try:
        process_online_retail(
            con,
            os.path.join(BRONZE_ROOT, 'Online_retail_data'),
            os.path.join(SILVER_ROOT, 'online_retail')
        )
        
        process_pos_billing(
            con,
            os.path.join(BRONZE_ROOT, 'pos_billing_data'),
            os.path.join(SILVER_ROOT, 'pos_billing')
        )

        process_warehouse(
            con,
            os.path.join(BRONZE_ROOT, 'warehouse_inventory_data'),
            os.path.join(SILVER_ROOT, 'warehouse_logs')
        )
    finally:
        con.close()
