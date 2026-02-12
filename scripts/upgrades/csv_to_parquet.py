import pandas as pd
import os
import sys

def process_file(file_path, output_root, timestamp_col, headers=None):
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
            # For live_sales_data.csv, headers might be missing
            if headers:
                df = pd.read_csv(file_path, names=headers, low_memory=False)
            else:
                df = pd.read_csv(file_path, low_memory=False)
        elif file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        else:
            print(f"Unsupported file type: {file_path}")
            return

        # Ensure timestamp column is datetime
        df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
        
        # Drop rows with invalid timestamps
        df = df.dropna(subset=[timestamp_col])

        # Create partitioning columns
        df['year'] = df[timestamp_col].dt.year
        df['month'] = df[timestamp_col].dt.month
        df['day'] = df[timestamp_col].dt.day

        # Ensure output directory exists (pandas will handle nested folders during partition)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        print(f"Partitioning and saving to {output_dir}...")
        # Write to Parquet using Hive partitioning
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
    
    # 1. Process live_sales_data.csv
    # Based on simulated_data.py, headers are:
    sales_headers = [
        "order_id", "store_id", "customer_id", "customer_city", "region", 
        "product_category", "channel", "quantity", "price_per_unit", 
        "discount", "payment_method", "holiday_flag", "order_status", 
        "total_amount", "timestamp"
    ]
    process_file('landing/live_sales_data.csv', BRONZE_ROOT, 'timestamp', headers=sales_headers)
    
    # 2. Process online_retail_II.xlsx
    # We force sensitive columns to string to avoid pyarrow conversion errors
    try:
        print("--- Special Handling for online_retail_II.xlsx ---")
        df_excel = pd.read_excel('landing/online_retail_II.xlsx')
        
        # Force all object-type columns to string to prevent pyarrow conversion issues
        # (Mixed types like int/str in a 'Description' or 'Invoice' column cause failure)
        for col in df_excel.select_dtypes(include=['object']).columns:
            df_excel[col] = df_excel[col].astype(str).replace('nan', '')
        
        # Reuse the partitioning logic
        timestamp_col = 'InvoiceDate'
        df_excel[timestamp_col] = pd.to_datetime(df_excel[timestamp_col], errors='coerce')
        df_excel = df_excel.dropna(subset=[timestamp_col])
        
        df_excel['year'] = df_excel[timestamp_col].dt.year
        df_excel['month'] = df_excel[timestamp_col].dt.month
        df_excel['day'] = df_excel[timestamp_col].dt.day
        
        output_dir = os.path.join(BRONZE_ROOT, 'online_retail_II')
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        print(f"Partitioning and saving to {output_dir}...")
        df_excel.to_parquet(
            output_dir, 
            engine='pyarrow', 
            compression='snappy', 
            index=False, 
            partition_cols=['year', 'month', 'day']
        )
        print("Successfully processed online_retail_II.xlsx into partitioned Parquet.")
        
    except Exception as e:
        print(f"An error occurred processing online_retail_II.xlsx: {e}")
