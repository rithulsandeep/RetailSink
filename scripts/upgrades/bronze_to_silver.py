import pandas as pd
import os

def clean_column_names(df):
    """Converts column names to lower_snake_case."""
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    return df

def process_live_sales(bronze_path, silver_path):
    print(f"--- Processing Live Sales Data (Bronze -> Silver) ---")
    df = pd.read_parquet(bronze_path)
    
    # 1. Clean column names
    df = clean_column_names(df)
    
    # 2. Deduplicate
    initial_count = len(df)
    df = df.drop_duplicates(subset=['order_id'])
    print(f"Removed {initial_count - len(df)} duplicate orders.")
    
    # 3. Data type casting
    df['order_id'] = df['order_id'].astype(str)
    df['store_id'] = df['store_id'].astype(str)
    df['customer_id'] = df['customer_id'].astype(str)
    
    # 4. Enrichment
    df['source_system'] = 'pos'
    df['is_cancelled'] = df['order_status'] == 'Cancelled'
    
    # 5. Save to Silver
    if not os.path.exists(silver_path):
        os.makedirs(silver_path)
        
    print(f"Saving to {silver_path}...")
    df.to_parquet(
        silver_path,
        engine='pyarrow',
        compression='snappy',
        index=False,
        partition_cols=['year', 'month', 'day']
    )
    print("Live Sales Data processed successfully.")

def process_online_retail(bronze_path, silver_path):
    print(f"--- Processing Online Retail Data (Bronze -> Silver) ---")
    df = pd.read_parquet(bronze_path)
    
    # 1. Rename and Clean columns
    df = df.rename(columns={
        'Invoice': 'invoice_id',
        'StockCode': 'product_id',
        'Description': 'product_description',
        'Quantity': 'quantity',
        'InvoiceDate': 'order_timestamp',
        'Price': 'unit_price',
        'Customer ID': 'customer_id',
        'Country': 'country'
    })
    df = clean_column_names(df)
    
    # 2. Handle Missing Values
    df['customer_id'] = df['customer_id'].fillna('Unknown').astype(str)
    df['invoice_id'] = df['invoice_id'].astype(str)
    df['product_id'] = df['product_id'].astype(str)
    
    # 3. Deduplicate
    initial_count = len(df)
    df = df.drop_duplicates()
    print(f"Removed {initial_count - len(df)} duplicate rows.")
    
    # 4. Enrichment
    df['source_system'] = 'erp'
    df['total_amount'] = (df['quantity'] * df['unit_price']).round(2)
    df['is_cancelled'] = df['invoice_id'].str.startswith('C')
    
    # 5. Save to Silver
    if not os.path.exists(silver_path):
        os.makedirs(silver_path)
        
    print(f"Saving to {silver_path}...")
    df.to_parquet(
        silver_path,
        engine='pyarrow',
        compression='snappy',
        index=False,
        partition_cols=['year', 'month', 'day']
    )
    print("Online Retail Data processed successfully.")

if __name__ == "__main__":
    BRONZE_ROOT = 'bronze'
    SILVER_ROOT = 'silver'
    
    # Process datasets
    process_live_sales(
        os.path.join(BRONZE_ROOT, 'live_sales_data'),
        os.path.join(SILVER_ROOT, 'live_sales_data')
    )
    
    process_online_retail(
        os.path.join(BRONZE_ROOT, 'online_retail_II'),
        os.path.join(SILVER_ROOT, 'online_retail_II')
    )
