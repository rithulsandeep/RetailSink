import pandas as pd
import duckdb
import os
import glob
import json

def get_row_counts():
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
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

    # Bronze (Delta)
    bronze_dirs = [
        ('medallion/bronze/Online_retail_data', 'Online Retail (Bronze)'),
        ('medallion/bronze/pos_billing_data', 'POS Billing (Bronze)'),
        ('medallion/bronze/warehouse_inventory_data', 'Warehouse (Bronze)')
    ]
    for path, name in bronze_dirs:
        if os.path.exists(path):
            try:
                count = con.execute(f"SELECT COUNT(*) FROM delta_scan('{path}')").fetchone()[0]
                stats.append({'layer': 'Bronze', 'name': name, 'count': count})
            except:
                pass

    # Silver (Delta)
    silver_dirs = [
        ('medallion/silver/online_retail', 'Online Retail (Silver)'),
        ('medallion/silver/pos_billing', 'POS Billing (Silver)'),
        ('medallion/silver/warehouse_logs', 'Warehouse (Silver)')
    ]
    for path, name in silver_dirs:
        if os.path.exists(path):
            try:
                count = con.execute(f"SELECT COUNT(*) FROM delta_scan('{path}')").fetchone()[0]
                stats.append({'layer': 'Silver', 'name': name, 'count': count})
            except:
                pass

    # Gold (Delta)
    gold_dirs = [
        ('medallion/gold/dim_product', 'dim_product (Gold)'),
        ('medallion/gold/dim_customer', 'dim_customer (Gold)'),
        ('medallion/gold/dim_date', 'dim_date (Gold)'),
        ('medallion/gold/fact_sales', 'fact_sales (Gold)'),
        ('medallion/gold/fact_inventory', 'fact_inventory (Gold)')
    ]
    for path, name in gold_dirs:
        if os.path.exists(path):
            try:
                count = con.execute(f"SELECT COUNT(*) FROM delta_scan('{path}')").fetchone()[0]
                stats.append({'layer': 'Gold', 'name': name, 'count': count})
            except:
                pass

    con.close()
    return stats

if __name__ == "__main__":
    stats = get_row_counts()
    with open('stats.json', 'w') as f:
        json.dump(stats, f, indent=4)
