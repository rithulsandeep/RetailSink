import duckdb
import os
import sys
# Add project root to sys.path to import pipeline.utils
sys.path.append(os.getcwd())
from pipeline.utils import get_incremental_scan_query, save_checkpoints, load_checkpoints
from deltalake import DeltaTable, write_deltalake

def init_duckdb():
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    return con

def merge_to_silver(con, silver_path, arrow_table, unique_keys, partition_cols, timestamp_col):
    """
    Merges data into Silver Delta table using Arrow.
    """
    if not arrow_table:
        print("No data to merge.")
        return
    
    # Check simple length or num_rows safely
    try:
        nrows = len(arrow_table)
    except:
        nrows = arrow_table.num_rows
        
    if nrows == 0:
        print("No data to merge.")
        return

    if not os.path.exists(silver_path):
        print(f"Creating new Silver table at {silver_path}")
        write_deltalake(silver_path, arrow_table, mode="overwrite", partition_by=partition_cols)
    else:
        print(f"Merging into existing Silver table at {silver_path}")
        dt = DeltaTable(silver_path)
        
        # Optimize Merge: Include partition columns in predicate to enable pruning
        # Ensure source has these columns
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
    print("Merge complete.")

def process_table(con, task_id, bronze_path, silver_path, query_template, merge_keys, partition_cols, timestamp_col):
    scan_query, new_version, has_updates = get_incremental_scan_query(bronze_path, task_id)
    
    if not has_updates:
        print(f"Skipping {task_id}: No new updates in Bronze (Version {new_version}).")
        return

    print(f"--- ELT: Processing {task_id} (Bronze v{new_version}) ---")
    
    # Inject scan query into template (replacing placeholder)
    query = query_template.replace("__SOURCE__", scan_query)
    
    try:
        # Use Arrow for zero-copy transfer
        # write_deltalake supports Arrow Table directly
        # arrow() returns a RecordBatchReader, we need to consume it to get a Table
        arrow_table = con.query(query).arrow().read_all()
        
        merge_to_silver(con, silver_path, arrow_table, merge_keys, partition_cols, timestamp_col)
        
        # Update checkpoint
        checkpoints = load_checkpoints()
        checkpoints[task_id] = new_version
        save_checkpoints(checkpoints)
        
    except Exception as e:
        print(f"Error processing {task_id}: {e}")
        raise e

def process_online_retail(con, bronze_path, silver_path):
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
    FROM __SOURCE__
    WHERE invoice_id IS NOT NULL 
      AND invoice_id != ''
      AND TRY_CAST(UnitPrice AS DOUBLE) > 0
    QUALIFY ROW_NUMBER() OVER(PARTITION BY invoice_id, product_id, quantity, order_timestamp ORDER BY order_timestamp DESC) = 1
    """
    process_table(con, "silver_online_retail", bronze_path, silver_path, query, 
                  ["invoice_id", "product_id", "quantity", "order_timestamp"], 
                  ["year", "month", "day"], "order_timestamp")

def process_pos_billing(con, bronze_path, silver_path):
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
    FROM __SOURCE__
    WHERE invoice_id IS NOT NULL 
      AND invoice_id != ''
      AND quantity IS NOT NULL
      AND unit_price IS NOT NULL
      AND unit_price > 0
    QUALIFY ROW_NUMBER() OVER(PARTITION BY invoice_id, product_id, quantity, order_timestamp ORDER BY order_timestamp DESC) = 1
    """
    process_table(con, "silver_pos_billing", bronze_path, silver_path, query, 
                  ["invoice_id", "product_id", "quantity", "order_timestamp"], 
                  ["year", "month", "day"], "order_timestamp")

def process_warehouse(con, bronze_path, silver_path):
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
    FROM __SOURCE__
    WHERE product_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY log_id ORDER BY log_timestamp DESC) = 1
    """
    process_table(con, "silver_warehouse", bronze_path, silver_path, query, 
                  ["log_id"], ["year", "month", "day"], "log_timestamp")

def process_shipments(con, bronze_path, silver_path):
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
    FROM __SOURCE__
    QUALIFY ROW_NUMBER() OVER(PARTITION BY invoice_id ORDER BY ship_timestamp DESC) = 1
    """
    process_table(con, "silver_shipments", bronze_path, silver_path, query, 
                  ["invoice_id"], ["year", "month", "day"], "ship_timestamp")


if __name__ == "__main__":
    import concurrent.futures
    import time

    BRONZE_ROOT = 'medallion/bronze'
    SILVER_ROOT = 'medallion/silver'
    
    # Wrapper to run with own connection
    def run_task(task_func, source, target):
        con = init_duckdb()
        try:
            task_func(con, source, target)
        except Exception as e:
            print(f"Error in {task_func.__name__}: {e}")
            raise e
        finally:
            con.close()

    start_time = time.time()
    print("--- Starting Parallel Silver Layer Processing ---")

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(run_task, process_online_retail, os.path.join(BRONZE_ROOT, 'Online_retail_data'), os.path.join(SILVER_ROOT, 'online_retail')),
            executor.submit(run_task, process_pos_billing, os.path.join(BRONZE_ROOT, 'pos_billing_data'), os.path.join(SILVER_ROOT, 'pos_billing')),
            executor.submit(run_task, process_warehouse, os.path.join(BRONZE_ROOT, 'warehouse_inventory_data'), os.path.join(SILVER_ROOT, 'warehouse_logs')),
            executor.submit(run_task, process_shipments, os.path.join(BRONZE_ROOT, 'shipments_data'), os.path.join(SILVER_ROOT, 'shipments'))
        ]
        
        # Wait for all
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Task failed: {e}")

    print(f"--- Silver Layer Finished in {time.time() - start_time:.2f}s ---")
