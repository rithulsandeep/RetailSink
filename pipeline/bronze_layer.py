import pandas as pd
import os
import sys
import json
from deltalake.writer import write_deltalake

# Checkpoint file path
CHECKPOINT_FILE = "data/pipeline_checkpoints.json"

def load_checkpoints():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_checkpoints(checkpoints):
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoints, f, indent=4)

def process_file(file_path, output_root, timestamp_col, custom_headers=None):
    """
    Processes a file (CSV or XLSX), adds partitioning columns, and appends to Delta table.
    Uses checkpoints to only process new rows from CSVs.
    """
    try:
        checkpoints = load_checkpoints()
        # Key for checkpoint is relative path or filename if unique
        # We'll use the provided file_path as key (assuming it's consistent)
        # Normalize path separators
        ckpt_key = file_path.replace("\\", "/")
        last_row_count = checkpoints.get(ckpt_key, 0)
        
        file_name = os.path.basename(file_path)
        base_name = os.path.splitext(file_name)[0]
        output_dir = os.path.join(output_root, base_name)
        
        print(f"--- Processing {file_name} ---")
        
        # Load data
        df = None
        current_row_count = 0
        
        if file_path.endswith('.csv'):
            if not os.path.exists(file_path):
                 print(f"File not found: {file_path}")
                 return

            # Get total lines efficiently? For now read all to get len for next time?
            # Or just read skiprows.
            # Issue: if we use skiprows, we don't know the new total length without reading.
            # Strategy: Read with skiprows. If empty, nothing new. 
            # New total length = last_row_count + len(df)
            
            try:
                # header=0 means first line is header. 
                # If we skip rows, we need to supply names if we want consistent columns,
                # OR we read header separately.
                # If last_row_count == 0, read normally.
                # If last_row_count > 0, skip rows. Logic:
                # CSV: Header is line 0. Data starts line 1.
                # processed=10 means we read 10 data rows.
                # Next read should start at line 11 (1-indexed data) -> line 12 (0-indexed file with header).
                # skiprows=N skips the first N lines of value.
                # If processed=0: skiprows=None.
                # If processed=10. Header(1) + 10 lines. Total 11 lines to skip.
                # So skiprows = last_row_count + 1 (if header present).
                
                rows_to_skip = 0
                if last_row_count > 0:
                    rows_to_skip = last_row_count + 1 # +1 for header
                    
                # We must provide names if we skip header, 
                # but pandas read_csv with skiprows and names will use names for the read data.
                # If we provide custom_headers, use them.
                
                read_params = {
                    "low_memory": False,
                    "names": custom_headers,
                    "header": 0 if last_row_count == 0 else None # Expect header explicitly at row 0 if start
                }
                
                if last_row_count > 0:
                    read_params["skiprows"] = rows_to_skip
                    # If we skip header, we MUST provide names/header logic.
                    if not custom_headers:
                        # Fallback: read header from first line of file just to get names?
                        # For now, relying on custom_headers being passed for all our files.
                        # If no custom_headers passed, we might face issue if skiprows > 0.
                        # Luckily all our calls pass custom_headers.
                        pass
                
                df = pd.read_csv(file_path, **read_params)
                
            except pd.errors.EmptyDataError:
                print("No new data found.")
                return

        elif file_path.endswith('.xlsx'):
            # Excel doesn't support efficient partial read easily.
            # We'll stick to full read for now or implement row check if needed.
            # Assuming XLSX files are small or we just overwrite.
            # But the requirement is incremental. 
            # For now, treat XLSX as full overwrite or skip if modified time hasn't changed?
            # User only mentioned CSVs mainly growing.
            # Let's just read full for XLSX for now.
            df = pd.read_excel(file_path)
            # Reset delta? Or overwrite? 
            # The prompt implies incremental. If data is appended to excel...
            # We'll assume Overwrite for Excel for safety or just process all and append?
            # If we append all, we duplicate.
            # Let's keep Excel as overwrite logic for now or implement dedup via Merge in Silver.
            # Given we are in Bronze (Raw), we usually just append. 
            # But if we read full file again, we duplicate everything.
            # Safe bet: Overwrite for XLSX (as they are usually small reference/static files in this context)
            # OR Checksums.
            pass
        else:
            print(f"Unsupported file type: {file_path}")
            return

        if df is None or df.empty:
            print("No new data rows to process.")
            return

        # Update row count
        new_rows = len(df)
        print(f"Read {new_rows} new rows.")
        
        # Ensure timestamp column is datetime
        if timestamp_col not in df.columns:
            print(f"Error: Timestamp column '{timestamp_col}' not found in {file_name}")
            print(f"Available columns: {df.columns.tolist()}")
            return

        df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
        
        # Drop rows with invalid timestamps
        orig_count = len(df)
        df = df.dropna(subset=[timestamp_col])
        if len(df) < orig_count:
            print(f"Dropped {orig_count - len(df)} rows with invalid timestamps.")

        # Create partitioning columns
        df['year'] = df[timestamp_col].dt.year
        df['month'] = df[timestamp_col].dt.month
        df['day'] = df[timestamp_col].dt.day

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        print(f"Appending to {output_dir} (Delta Lake format)...")
        
        # Mode is append for CSV incremental. Overwrite for global refresh if needed (but we want incremental)
        # If it's the first run (last_row_count == 0), maybe overwrite to be safe? 
        # But if table exists, overwrite clears history.
        # Ideally: 
        # If last_row_count == 0: mode="overwrite" (reset)? -> No, what if we restart script but table persists?
        # If Checkpoint says 0, we assume we are reading from start.
        # If table exists and we read from start, we should probably overwrite to avoid duplication,
        # OR we assume the table is empty.
        # Secure approach: If last_row_count == 0, use overwrite. If > 0, append.
        
        mode = "append"
        if last_row_count == 0:
            mode = "overwrite"
            
        write_deltalake(
            output_dir,
            df,
            mode=mode,
            partition_by=["year", "month", "day"]
        )
        print(f"Successfully appended {new_rows} rows to Delta Lake table.")
        
        # Update checkpoint
        checkpoints[ckpt_key] = last_row_count + new_rows
        save_checkpoints(checkpoints)
        
    except Exception as e:
        print(f"An error occurred processing {file_path}: {e}")

if __name__ == "__main__":
    # Root directory for bronze layer
    BRONZE_ROOT = 'medallion/bronze'
    
    # 1. Process Online Retail (CSV version)
    online_retail_headers = [
        "InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", 
        "UnitPrice", "CustomerID", "Country", "City", "Cost_Price", 
        "Initial_Stock_Level", "Delivery_Date"
    ]
    process_file('landing/Online_retail_data.csv', BRONZE_ROOT, 'InvoiceDate', custom_headers=online_retail_headers)
    
    # 2. Process POS Billing Data
    pos_headers = [
        "BillNo", "ItemCode", "ProductName", "Qty", "BillDate", 
        "Rate", "LoyaltyID", "Nation", "StoreCity", "BuyPrice", 
        "StockAtHand", "PayMode", "CashierID", "ShopTaxRate"
    ]
    process_file('landing/pos_billing_data.csv', BRONZE_ROOT, 'BillDate', custom_headers=pos_headers)
    
    # 3. Process Warehouse Inventory Logs
    warehouse_headers = [
        "LogID", "WarehouseCode", "SKU_ID", "Item_Name", "BatchNo", 
        "MovementType", "Quantity_Change", "EventDate", "SupplierName", 
        "CountryOfOrigin", "PackageWeight_kg", "StorageTemp_C", "BinLocation"
    ]
    process_file('landing/warehouse_inventory_data.csv', BRONZE_ROOT, 'EventDate', custom_headers=warehouse_headers)
    # 4. Process Shipment Data
    shipment_headers = [
        "invoice_id", "order_timestamp", "ship_timestamp", "delivery_timestamp", "city", "country"
    ]
    process_file('landing/shipments_data.csv', BRONZE_ROOT, 'ship_timestamp', custom_headers=shipment_headers)
