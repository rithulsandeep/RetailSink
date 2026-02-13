from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import duckdb
import pandas as pd
import json
import os
import threading

app = FastAPI(title="RetailSink Analytics API")

# Enable CORS for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Paths to Gold layer data
FACT_SALES = "medallion/gold/fact_sales"
FACT_INVENTORY = "medallion/gold/fact_inventory"
FACT_SHIPMENTS = "medallion/gold/fact_shipments"
DIM_PRODUCT = "medallion/gold/dim_product"
DIM_CUSTOMER = "medallion/gold/dim_customer"
DIM_DATE = "medallion/gold/dim_date"
KPI_SUMMARY = "medallion/gold/kpi_summary"

# Persistent DuckDB connection initialized once at startup
db_conn = duckdb.connect()
db_conn.execute("INSTALL delta; LOAD delta;")
db_lock = threading.Lock()

# Result Cache
cache = {}

def create_views():
    """Create or refresh DuckDB views for all Delta tables to cache metadata."""
    global cache
    cache = {} # Clear result cache when views refresh
    tables = {
        "fact_sales": "medallion/gold/fact_sales",
        "fact_inventory": "medallion/gold/fact_inventory",
        "fact_shipments": "medallion/gold/fact_shipments",
        "dim_product": "medallion/gold/dim_product",
        "dim_customer": "medallion/gold/dim_customer",
        "dim_date": "medallion/gold/dim_date",
        "kpi_summary": "medallion/gold/kpi_summary",
        "bronze_retail": "medallion/bronze/Online_retail_data",
        "bronze_pos": "medallion/bronze/pos_billing_data",
        "bronze_wh": "medallion/bronze/warehouse_inventory_data",
        "bronze_ship": "medallion/bronze/shipments_data",
        "silver_retail": "medallion/silver/online_retail",
        "silver_pos": "medallion/silver/pos_billing",
        "silver_wh": "medallion/silver/warehouse_logs",
        "silver_ship": "medallion/silver/shipments"
    }
    with db_lock:
        for view_name, path in tables.items():
            if os.path.exists(path):
                db_conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM delta_scan('{path}')")
    print("DuckDB Views Refreshed & Cache Cleared.")

# Initial view creation
create_views()

@app.get("/api/admin/refresh")
def refresh_data():
    """Manual trigger to refresh Delta metadata (views) and clear cache."""
    create_views()
    return {"status": "success", "message": "Delta views refreshed and cache cleared"}

@app.get("/api/kpi/summary")
def get_summary_kpis():
    if "summary" in cache: return cache["summary"]
    query = "SELECT * FROM kpi_summary"
    with db_lock:
        res = db_conn.query(query).to_df().to_dict(orient='records')[0]
    cache["summary"] = res
    return res

@app.get("/api/kpi/revenue-trend")
def get_revenue_trend(period: str = "month"):
    key = f"revenue_trend_{period}"
    if key in cache: return cache[key]
    group_col = "month" if period == "month" else "day"
        
    query = f"""
    SELECT 
        {group_col}, 
        year,
        SUM(total_amount) as revenue
    FROM fact_sales
    GROUP BY {group_col}, year
    ORDER BY year DESC, {group_col} DESC
    LIMIT 12
    """
    with db_lock:
        df = db_conn.query(query).to_df()
    df = df.sort_values(['year', period])
    res = df.to_dict(orient='records')
    cache[key] = res
    return res

@app.get("/api/kpi/top-products")
def get_top_products(limit: int = 5):
    key = f"top_products_{limit}"
    if key in cache: return cache[key]
    query = f"""
    SELECT 
        p.product_description,
        SUM(s.quantity) as total_quantity,
        SUM(s.total_amount) as total_revenue
    FROM fact_sales s
    JOIN dim_product p ON s.product_key = p.product_key
    GROUP BY p.product_description
    ORDER BY total_revenue DESC
    LIMIT {limit}
    """
    with db_lock:
        res = db_conn.query(query).to_df().to_dict(orient='records')
    cache[key] = res
    return res

@app.get("/api/kpi/city-sales")
def get_city_sales():
    if "city_sales" in cache: return cache["city_sales"]
    query = f"""
    SELECT 
        city,
        SUM(total_amount) as revenue
    FROM fact_sales
    GROUP BY city
    ORDER BY revenue DESC
    LIMIT 10
    """
    with db_lock:
        res = db_conn.query(query).to_df().to_dict(orient='records')
    cache["city_sales"] = res
    return res

@app.get("/api/kpi/operations-metrics")
def get_operations_metrics():
    if "ops_metrics" in cache: return cache["ops_metrics"]
    
    delivery_query = "SELECT AVG(delivery_days) as avg_delivery_days FROM fact_shipments"
    turnover_query = """
    SELECT 
        (SELECT SUM(total_amount) FROM fact_sales) / 
        COALESCE(NULLIF(SUM(ABS(qty_change)), 0), 1) as turnover_ratio
    FROM fact_inventory
    """
    seasonal_query = """
    SELECT month, SUM(total_amount) as revenue 
    FROM fact_sales
    GROUP BY month ORDER BY month
    """
    
    with db_lock:
        avg_delivery = db_conn.query(delivery_query).to_df().iloc[0,0]
        turnover = db_conn.query(turnover_query).to_df().iloc[0,0]
        seasonal = db_conn.query(seasonal_query).to_df().to_dict(orient='records')
    
    res = {
        "avg_delivery_days": round(float(avg_delivery), 2) if avg_delivery else 0,
        "turnover_ratio": round(float(turnover), 2) if turnover else 0,
        "seasonal_demand": seasonal
    }
    cache["ops_metrics"] = res
    return res

@app.get("/api/kpi/customer-insights")
def get_customer_insights():
    if "cust_insights" in cache: return cache["cust_insights"]
    
    retention_query = """
    WITH cust_orders AS (
        SELECT customer_key, COUNT(DISTINCT invoice_id) as order_count
        FROM fact_sales
        GROUP BY customer_key
    )
    SELECT 
        CASE WHEN order_count > 1 THEN 'Returning' ELSE 'New' END as segment,
        COUNT(*) as count
    FROM cust_orders
    GROUP BY Segment
    """
    clv_query = """
    SELECT AVG(customer_revenue) as clv
    FROM (
        SELECT customer_key, SUM(total_amount) as customer_revenue
        FROM fact_sales
        GROUP BY customer_key
    )
    """
    basket_query = """
    WITH target_invoices AS (
        SELECT DISTINCT invoice_id 
        FROM fact_sales
        LIMIT 2000
    )
    SELECT 
        p1.product_description as item_a,
        p2.product_description as item_b,
        COUNT(*) as frequency
    FROM fact_sales s1
    JOIN target_invoices t ON s1.invoice_id = t.invoice_id
    JOIN fact_sales s2 ON s1.invoice_id = s2.invoice_id AND s1.product_key < s2.product_key
    JOIN dim_product p1 ON s1.product_key = p1.product_key
    JOIN dim_product p2 ON s2.product_key = p2.product_key
    GROUP BY item_a, item_b
    ORDER BY frequency DESC
    LIMIT 5
    """
    
    with db_lock:
        segments = db_conn.query(retention_query).to_df().to_dict(orient='records')
        clv = db_conn.query(clv_query).to_df().iloc[0,0]
        basket = db_conn.query(basket_query).to_df().to_dict(orient='records')
    
    res = {
        "segments": segments,
        "clv": round(float(clv), 2) if clv else 0,
        "market_basket": basket
    }
    cache["cust_insights"] = res
    return res

@app.get("/api/kpi/sales-channel")
def get_sales_channel_distribution():
    if "channel_dist" in cache: return cache["channel_dist"]
    query = "SELECT source_channel, SUM(total_amount) as revenue FROM fact_sales GROUP BY source_channel"
    with db_lock:
        res = db_conn.query(query).to_df().to_dict(orient='records')
    cache["channel_dist"] = res
    return res

@app.get("/api/kpi/inventory-status")
def get_inventory_status():
    if "inv_status" in cache: return cache["inv_status"]
    query = """
    SELECT 
        p.product_description,
        SUM(i.qty_change) as current_stock
    FROM fact_inventory i
    JOIN dim_product p ON i.product_key = p.product_key
    GROUP BY p.product_description
    HAVING current_stock > 0
    ORDER BY current_stock DESC
    LIMIT 10
    """
    with db_lock:
        res = db_conn.query(query).to_df().to_dict(orient='records')
    cache["inv_status"] = res
    return res

@app.get("/api/kpi/lineage-stats")
def get_lineage_stats():
    if "lineage_stats" in cache: return cache["lineage_stats"]
    stats = []
    
    # Landing (CSVs)
    landing_paths = {
        "Online Retail (Landing)": "landing/Online_retail_data.csv",
        "POS Billing (Landing)": "landing/pos_billing_data.csv",
        "Warehouse (Landing)": "landing/warehouse_inventory_data.csv",
        "Shipments (Landing)": "landing/shipments_data.csv"
    }
    with db_lock:
        for name, path in landing_paths.items():
            if os.path.exists(path):
                try:
                    count = db_conn.execute(f"SELECT COUNT(*) FROM read_csv_auto('{path}')").fetchone()[0]
                    stats.append({"layer": "Landing", "name": name, "count": count})
                except: pass

        # Bronze to Gold using Views
        layers = [
            ("Bronze", {"Retail": "bronze_retail", "POS": "bronze_pos", "WH": "bronze_wh", "Ship": "bronze_ship"}),
            ("Silver", {"Retail": "silver_retail", "POS": "silver_pos", "WH": "silver_wh", "Ship": "silver_ship"}),
            ("Gold", {"dim_product": "dim_product", "dim_customer": "dim_customer", "fact_sales": "fact_sales", "fact_inventory": "fact_inventory"})
        ]
        
        for layer_name, entities in layers:
            for name, view in entities.items():
                try:
                    count = db_conn.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]
                    stats.append({"layer": layer_name, "name": f"{name} ({layer_name})", "count": count})
                except: pass

    cache["lineage_stats"] = stats
    return stats

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
