from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import duckdb
import pandas as pd
import json

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
