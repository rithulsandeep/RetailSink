import duckdb
import os
from deltalake.writer import write_deltalake

def init_duckdb():
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    return con

def process_online_retail(con, bronze_path, silver_path):
    print(f"--- ELT: Normalizing Online Retail (ERP) to Delta ---")
    
    # We use delta_scan for the input since csv_to_parquet now writes Delta
    query = f"""
    SELECT 
        REGEXP_REPLACE(TRIM(InvoiceNo), '^C', '', 'i')::VARCHAR as invoice_id,
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
        (TRIM(InvoiceNo) LIKE 'C%') as is_cancelled,
        'Unknown'::VARCHAR as city,
        COALESCE(year, YEAR(order_timestamp)) as year,
        COALESCE(month, MONTH(order_timestamp)) as month,
        COALESCE(day, DAY(order_timestamp)) as day
    FROM delta_scan('{bronze_path}')
    WHERE invoice_id IS NOT NULL 
      AND invoice_id != ''
      AND TRY_CAST(UnitPrice AS DOUBLE) > 0
    QUALIFY ROW_NUMBER() OVER(PARTITION BY invoice_id, product_id, quantity, order_timestamp) = 1
    """
    df = con.query(query).to_df()
    write_deltalake(silver_path, df, mode="overwrite", partition_by=["year", "month", "day"])
    print(f"Successfully normalized Online Retail to Delta table at {silver_path}")

def process_pos_billing(con, bronze_path, silver_path):
    print(f"--- ELT: Normalizing POS Billing (In-Store) to Delta ---")
    
    query = f"""
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
        TRIM(StoreCity) as city,
        COALESCE(year, YEAR(order_timestamp)) as year,
        COALESCE(month, MONTH(order_timestamp)) as month,
        COALESCE(day, DAY(order_timestamp)) as day
    FROM delta_scan('{bronze_path}')
    WHERE invoice_id IS NOT NULL 
      AND invoice_id != ''
      AND quantity IS NOT NULL
      AND unit_price IS NOT NULL
      AND unit_price > 0
    QUALIFY ROW_NUMBER() OVER(PARTITION BY invoice_id, product_id, quantity, order_timestamp) = 1
    """
    df = con.query(query).to_df()
    write_deltalake(silver_path, df, mode="overwrite", partition_by=["year", "month", "day"])
    print(f"Successfully normalized POS Billing to Delta table at {silver_path}")

def process_warehouse(con, bronze_path, silver_path):
    print(f"--- ELT: Normalizing Warehouse Logs to Delta ---")
    
    query = f"""
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
    FROM delta_scan('{bronze_path}')
    WHERE product_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY log_id) = 1
    """
    df = con.query(query).to_df()
    write_deltalake(silver_path, df, mode="overwrite", partition_by=["year", "month", "day"])
    print(f"Successfully normalized Warehouse Logs to Delta table at {silver_path}")

def process_shipments(con, bronze_path, silver_path):
    print(f"--- ELT: Normalizing Shipment Data to Delta ---")
    
    query = f"""
    SELECT 
        REGEXP_REPLACE(TRIM(invoice_id), '^C', '', 'i') as invoice_id,
        ship_timestamp,
        delivery_timestamp,
        TRIM(city) as city,
        TRIM(country) as country,
        year,
        month,
        day
    FROM delta_scan('{bronze_path}')
    QUALIFY ROW_NUMBER() OVER(PARTITION BY invoice_id) = 1
    """
    df = con.query(query).to_df()
    write_deltalake(silver_path, df, mode="overwrite", partition_by=["year", "month", "day"])
    print(f"Successfully normalized Shipments to Delta table at {silver_path}")

if __name__ == "__main__":
    BRONZE_ROOT = 'medallion/bronze'
    SILVER_ROOT = 'medallion/silver'
    
    con = init_duckdb()
    
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

        process_shipments(
            con,
            os.path.join(BRONZE_ROOT, 'shipments_data'),
            os.path.join(SILVER_ROOT, 'shipments')
        )
    finally:
        con.close()
