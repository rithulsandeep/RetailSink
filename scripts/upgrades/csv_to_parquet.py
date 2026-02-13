import pandas as pd
import os
import sys

def process_file(file_path, output_root, timestamp_col, custom_headers=None):
    """
    Processes a file (CSV or XLSX), adds partitioning columns, and saves as Parquet.
    """
    try:
        file_name = os.path.basename(file_path)
        base_name = os.path.splitext(file_name)[0]
        output_dir = os.path.join(output_root, base_name)
        
        print(f"--- Processing {file_name} ---")
        
        # Load data
        if file_path.endswith('.csv'):
            # Read normally and rename if custom_headers match
            df = pd.read_csv(file_path, low_memory=False)
            if custom_headers:
                if len(df.columns) == len(custom_headers):
                    df.columns = custom_headers
                else:
                    print(f"Warning: Custom headers count ({len(custom_headers)}) doesn't match file column count ({len(df.columns)}). Using file headers.")
        elif file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        else:
            print(f"Unsupported file type: {file_path}")
            return

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

        print(f"Partitioning and saving to {output_dir}...")
        df.to_parquet(
            output_dir, 
            engine='pyarrow', 
            compression='snappy', 
            index=False, 
            partition_cols=['year', 'month', 'day']
        )
        print(f"Successfully processed {file_name} into partitioned Parquet.")
        
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
