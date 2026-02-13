from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import duckdb
import pandas as pd
import json
import os

app = FastAPI(title="Retail Analytics API")

# Enable CORS for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Paths to Gold layer data
FACT_SALES = "medallion/gold/fact_sales/**/*.parquet"
FACT_INVENTORY = "medallion/gold/fact_inventory/**/*.parquet"
DIM_PRODUCT = "medallion/gold/dim_product.parquet"
DIM_CUSTOMER = "medallion/gold/dim_customer.parquet"
DIM_DATE = "medallion/gold/dim_date.parquet"

def get_db():
    return duckdb.connect()

@app.get("/api/kpi/summary")
def get_summary_kpis():
    db = get_db()
    
    # Total Revenue, Total Orders, Total Customers
    query = f"""
    SELECT 
        SUM(total_amount) as total_revenue,
        COUNT(DISTINCT invoice_id) as total_orders,
        COUNT(DISTINCT customer_key) as total_customers
    FROM read_parquet('{FACT_SALES}', hive_partitioning = true)
    """
    res = db.query(query).to_df().to_dict(orient='records')[0]
    return res

@app.get("/api/kpi/revenue-trend")
def get_revenue_trend(period: str = "month"):
    db = get_db()
    if period == "month":
        group_col = "month"
    else:
        group_col = "day"
        
    query = f"""
    SELECT 
        {group_col}, 
        year,
        SUM(total_amount) as revenue
    FROM read_parquet('{FACT_SALES}', hive_partitioning = true)
    GROUP BY {group_col}, year
    ORDER BY year DESC, {group_col} DESC
    LIMIT 12
    """
    df = db.query(query).to_df()
    # Sort for chart display
    df = df.sort_values(['year', period])
    return df.to_dict(orient='records')

@app.get("/api/kpi/top-products")
def get_top_products(limit: int = 5):
    db = get_db()
    query = f"""
    SELECT 
        p.product_description,
        SUM(s.quantity) as total_quantity,
        SUM(s.total_amount) as total_revenue
    FROM read_parquet('{FACT_SALES}', hive_partitioning = true) s
    JOIN read_parquet('{DIM_PRODUCT}') p ON s.product_key = p.product_key
    GROUP BY p.product_description
    ORDER BY total_revenue DESC
    LIMIT {limit}
    """
    return db.query(query).to_df().to_dict(orient='records')

@app.get("/api/kpi/sales-channel")
def get_sales_channel_distribution():
    db = get_db()
    query = f"""
    SELECT 
        source_channel,
        SUM(total_amount) as revenue
    FROM read_parquet('{FACT_SALES}', hive_partitioning = true)
    GROUP BY source_channel
    """
    return db.query(query).to_df().to_dict(orient='records')

@app.get("/api/kpi/inventory-status")
def get_inventory_status():
    db = get_db()
    query = f"""
    SELECT 
        p.product_description,
        SUM(i.qty_change) as current_stock
    FROM read_parquet('{FACT_INVENTORY}', hive_partitioning = true) i
    JOIN read_parquet('{DIM_PRODUCT}') p ON i.product_key = p.product_key
    GROUP BY p.product_description
    HAVING current_stock > 0
    ORDER BY current_stock DESC
    LIMIT 10
    """
    return db.query(query).to_df().to_dict(orient='records')

@app.get("/api/kpi/lineage-stats")
def get_lineage_stats():
    db = get_db()
    
    stats = []
    
    # 1. Landing (CSVs) - Use duckdb for counts
    landing_paths = {
        "Online Retail (Landing)": "landing/Online_retail_data.csv",
        "POS Billing (Landing)": "landing/pos_billing_data.csv",
        "Warehouse (Landing)": "landing/warehouse_inventory_data.csv"
    }
    
    for name, path in landing_paths.items():
        if os.path.exists(path):
            try:
                # DuckDB reading CSV for count is faster than pandas
                count = db.execute(f"SELECT COUNT(*) FROM read_csv_auto('{path}')").fetchone()[0]
                stats.append({"layer": "Landing", "name": name, "count": count})
            except Exception as e:
                print(f"Error counting {path}: {e}")

    # 2. Bronze
    bronze_paths = {
        "Online Retail (Bronze)": "medallion/bronze/Online_retail_data",
        "POS Billing (Bronze)": "medallion/bronze/pos_billing_data",
        "Warehouse (Bronze)": "medallion/bronze/warehouse_inventory_data"
    }
    for name, path in bronze_paths.items():
        if os.path.exists(path):
            count = db.execute(f"SELECT COUNT(*) FROM read_parquet('{path}/**/*.parquet')").fetchone()[0]
            stats.append({"layer": "Bronze", "name": name, "count": count})

    # 3. Silver
    silver_paths = {
        "Online Retail (Silver)": "medallion/silver/online_retail",
        "POS Billing (Silver)": "medallion/silver/pos_billing",
        "Warehouse (Silver)": "medallion/silver/warehouse_logs"
    }
    for name, path in silver_paths.items():
        if os.path.exists(path):
            count = db.execute(f"SELECT COUNT(*) FROM read_parquet('{path}/**/*.parquet')").fetchone()[0]
            stats.append({"layer": "Silver", "name": name, "count": count})

    # 4. Gold
    gold_entities = {
        "dim_product (Gold)": DIM_PRODUCT,
        "dim_customer (Gold)": DIM_CUSTOMER,
        "dim_date (Gold)": DIM_DATE,
        "fact_sales (Gold)": FACT_SALES,
        "fact_inventory (Gold)": FACT_INVENTORY
    }
    for name, path in gold_entities.items():
        try:
            count = db.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
            stats.append({"layer": "Gold", "name": name, "count": count})
        except:
            pass

    return stats

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
