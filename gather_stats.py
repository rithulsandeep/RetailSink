import pandas as pd
import duckdb
import os
import glob
import json

def get_row_counts():
    con = duckdb.connect()
    stats = []

    # Landing (CSV/XLSX)
    landing_files = [
        ('landing/Online_retail_data.csv', 'Online Retail (CSV)'),
        ('landing/pos_billing_data.csv', 'POS Billing (CSV)'),
        ('landing/warehouse_inventory_data.csv', 'Warehouse (CSV)')
    ]
    for path, name in landing_files:
        if os.path.exists(path):
            try:
                df = pd.read_csv(path, low_memory=False)
                stats.append({'layer': 'Landing', 'name': name, 'count': len(df)})
            except:
                pass

    # Bronze (Parquet)
    bronze_dirs = [
        ('medallion/bronze/Online_retail_data', 'Online Retail (Bronze)'),
        ('medallion/bronze/pos_billing_data', 'POS Billing (Bronze)'),
        ('medallion/bronze/warehouse_inventory_data', 'Warehouse (Bronze)')
    ]
    for path, name in bronze_dirs:
        if os.path.exists(path):
            count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}/**/*.parquet')").fetchone()[0]
            stats.append({'layer': 'Bronze', 'name': name, 'count': count})

    # Silver (Parquet)
    silver_dirs = [
        ('medallion/silver/online_retail', 'Online Retail (Silver)'),
        ('medallion/silver/pos_billing', 'POS Billing (Silver)'),
        ('medallion/silver/warehouse_logs', 'Warehouse (Silver)')
    ]
    for path, name in silver_dirs:
        if os.path.exists(path):
            count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}/**/*.parquet')").fetchone()[0]
            stats.append({'layer': 'Silver', 'name': name, 'count': count})

    # Gold (Parquet)
    gold_files = [
        ('medallion/gold/dim_product.parquet', 'dim_product (Gold)'),
        ('medallion/gold/dim_customer.parquet', 'dim_customer (Gold)'),
        ('medallion/gold/dim_date.parquet', 'dim_date (Gold)'),
        ('medallion/gold/fact_sales/**/*.parquet', 'fact_sales (Gold)'),
        ('medallion/gold/fact_inventory/**/*.parquet', 'fact_inventory (Gold)')
    ]
    for path, name in gold_files:
        files = glob.glob(path, recursive=True)
        if files:
            count = con.execute(f"SELECT COUNT(*) FROM read_parquet({files})").fetchone()[0]
            stats.append({'layer': 'Gold', 'name': name, 'count': count})

    con.close()
    return stats

if __name__ == "__main__":
    stats = get_row_counts()
    with open('stats.json', 'w') as f:
        json.dump(stats, f, indent=4)
