import duckdb
import os
from deltalake import DeltaTable, write_deltalake

def init_duckdb():
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    return con

def get_bronze_scan(bronze_path):
    # For now, we scan the full bronze table.
    # To be truly incremental from Bronze to Silver, we would need to track Bronze table versions.
    # However, since Bronze is Append-Only, we can also use MERGE in Silver to handle "New" records.
    # If we want to avoid reading full Bronze, we'd need:
    # SELECT * FROM delta_scan('path') WHERE _commit_timestamp > last_run
    # For this iteration, we will read Bronze (which is growing) and use MERGE in Silver to be idempotent.
    return f"delta_scan('{bronze_path}')"

def merge_to_silver(con, silver_path, df, unique_keys, partition_cols, timestamp_col):
    """
    Merges data into Silver Delta table.
    If table doesn't exist, create it.
    If exists, MERGE based on unique keys.
    """
    if df.empty:
        print("No data to merge.")
        return

    if not os.path.exists(silver_path):
        print(f"Creating new Silver table at {silver_path}")
        write_deltalake(silver_path, df, mode="overwrite", partition_by=partition_cols)
    else:
        print(f"Merging into existing Silver table at {silver_path}")
        dt = DeltaTable(silver_path)
        
        # Construct merge predicate
        # source.key = target.key
        predicate = " AND ".join([f"s.{k} = t.{k}" for k in unique_keys])
        
        # DuckDB -> Arrow -> Delta Merge
        # DeltaTable.merge() in python requires:
        # source: pyarrow table, pandas df, etc.
        # predicate: str
        # source_alias: str, target_alias: str
        # when_matched_update_all()
        # when_not_matched_insert_all()
        
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
    print("Merge complete.")

def process_online_retail(con, bronze_path, silver_path):
    print(f"--- ELT: Normalizing Online Retail (ERP) to Delta ---")
    
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
    FROM {get_bronze_scan(bronze_path)}
    WHERE invoice_id IS NOT NULL 
      AND invoice_id != ''
      AND TRY_CAST(UnitPrice AS DOUBLE) > 0
    QUALIFY ROW_NUMBER() OVER(PARTITION BY invoice_id, product_id, quantity, order_timestamp ORDER BY order_timestamp DESC) = 1
    """
    # Note: We filter duplicates in the source query before merge
    
    df = con.query(query).to_df()
    
    # Merge Keys: Composite key to identify unique line item?
    # invoice_id + product_id is usually unique per invoice, but quantity could differ?
    # In retail data, sometimes same product appears twice in invoice?
    # Let's assume invoice_id, product_id, quantity, order_timestamp is Row ID.
    merge_keys = ["invoice_id", "product_id", "quantity", "order_timestamp"]
    
    merge_to_silver(con, silver_path, df, merge_keys, ["year", "month", "day"], "order_timestamp")

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
    FROM {get_bronze_scan(bronze_path)}
    WHERE invoice_id IS NOT NULL 
      AND invoice_id != ''
      AND quantity IS NOT NULL
      AND unit_price IS NOT NULL
      AND unit_price > 0
    QUALIFY ROW_NUMBER() OVER(PARTITION BY invoice_id, product_id, quantity, order_timestamp ORDER BY order_timestamp DESC) = 1
    """
    df = con.query(query).to_df()
    merge_keys = ["invoice_id", "product_id", "quantity", "order_timestamp"]
    merge_to_silver(con, silver_path, df, merge_keys, ["year", "month", "day"], "order_timestamp")

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
    FROM {get_bronze_scan(bronze_path)}
    WHERE product_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY log_id ORDER BY log_timestamp DESC) = 1
    """
    df = con.query(query).to_df()
    # LogID should be unique
    merge_keys = ["log_id"]
    merge_to_silver(con, silver_path, df, merge_keys, ["year", "month", "day"], "log_timestamp")

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
    FROM {get_bronze_scan(bronze_path)}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY invoice_id ORDER BY ship_timestamp DESC) = 1
    """
    df = con.query(query).to_df()
    merge_keys = ["invoice_id"]
    merge_to_silver(con, silver_path, df, merge_keys, ["year", "month", "day"], "ship_timestamp")

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
