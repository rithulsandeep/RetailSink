from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import duckdb
import pandas as pd
import json
import os

app = FastAPI(title="RetailSink Analytics API")

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
FACT_SHIPMENTS = "medallion/gold/fact_shipments/**/*.parquet"
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

@app.get("/api/kpi/city-sales")
def get_city_sales():
    db = get_db()
    query = f"""
    SELECT 
        city,
        SUM(total_amount) as revenue
    FROM read_parquet('{FACT_SALES}', hive_partitioning = true)
    GROUP BY city
    ORDER BY revenue DESC
    LIMIT 10
    """
    return db.query(query).to_df().to_dict(orient='records')

@app.get("/api/kpi/operations-metrics")
def get_operations_metrics():
    db = get_db()
    
    # 1. Avg Delivery Time
    delivery_query = f"SELECT AVG(delivery_days) as avg_delivery_days FROM read_parquet('{FACT_SHIPMENTS}', hive_partitioning = true)"
    avg_delivery = db.query(delivery_query).to_df().iloc[0,0]
    
    # 2. Inventory Turnover (Simplified: Total Revenue / Total Qty Change)
    turnover_query = f"""
    SELECT 
        (SELECT SUM(total_amount) FROM read_parquet('{FACT_SALES}', hive_partitioning = true)) / 
        COALESCE(NULLIF(SUM(ABS(qty_change)), 0), 1) as turnover_ratio
    FROM read_parquet('{FACT_INVENTORY}', hive_partitioning = true)
    """
    turnover = db.query(turnover_query).to_df().iloc[0,0]
    
    # 3. Seasonal Demand (Revenue by Month)
    seasonal_query = f"""
    SELECT month, SUM(total_amount) as revenue 
    FROM read_parquet('{FACT_SALES}', hive_partitioning = true)
    GROUP BY month ORDER BY month
    """
    seasonal = db.query(seasonal_query).to_df().to_dict(orient='records')
    
    return {
        "avg_delivery_days": round(float(avg_delivery), 2) if avg_delivery else 0,
        "turnover_ratio": round(float(turnover), 2) if turnover else 0,
        "seasonal_demand": seasonal
    }

@app.get("/api/kpi/customer-insights")
def get_customer_insights():
    db = get_db()
    
    # 1. New vs Returning (Frequency of purchases)
    retention_query = f"""
    WITH cust_orders AS (
        SELECT customer_key, COUNT(DISTINCT invoice_id) as order_count
        FROM read_parquet('{FACT_SALES}', hive_partitioning = true)
        GROUP BY customer_key
    )
    SELECT 
        CASE WHEN order_count > 1 THEN 'Returning' ELSE 'New' END as segment,
        COUNT(*) as count
    FROM cust_orders
    GROUP BY Segment
    """
    segments = db.query(retention_query).to_df().to_dict(orient='records')
    
    # 2. CLV (Avg Revenue per Customer)
    clv_query = f"""
    SELECT AVG(customer_revenue) as clv
    FROM (
        SELECT customer_key, SUM(total_amount) as customer_revenue
        FROM read_parquet('{FACT_SALES}', hive_partitioning = true)
        GROUP BY customer_key
    )
    """
    clv = db.query(clv_query).to_df().iloc[0,0]
    
    # 3. Market Basket (Optimized with sampling)
    basket_query = f"""
    WITH target_invoices AS (
        SELECT DISTINCT invoice_id 
        FROM read_parquet('{FACT_SALES}', hive_partitioning = true)
        LIMIT 2000
    )
    SELECT 
        p1.product_description as item_a,
        p2.product_description as item_b,
        COUNT(*) as frequency
    FROM read_parquet('{FACT_SALES}', hive_partitioning = true) s1
    JOIN target_invoices t ON s1.invoice_id = t.invoice_id
    JOIN read_parquet('{FACT_SALES}', hive_partitioning = true) s2 ON s1.invoice_id = s2.invoice_id AND s1.product_key < s2.product_key
    JOIN read_parquet('{DIM_PRODUCT}') p1 ON s1.product_key = p1.product_key
    JOIN read_parquet('{DIM_PRODUCT}') p2 ON s2.product_key = p2.product_key
    GROUP BY item_a, item_b
    ORDER BY frequency DESC
    LIMIT 5
    """
    basket = db.query(basket_query).to_df().to_dict(orient='records')
    
    return {
        "segments": segments,
        "clv": round(float(clv), 2) if clv else 0,
        "market_basket": basket
    }

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
    
    # 1. Landing (CSVs)
    landing_paths = {
        "Online Retail (Landing)": "landing/Online_retail_data.csv",
        "POS Billing (Landing)": "landing/pos_billing_data.csv",
        "Warehouse (Landing)": "landing/warehouse_inventory_data.csv",
        "Shipments (Landing)": "landing/shipments_data.csv"
    }
    
    for name, path in landing_paths.items():
        if os.path.exists(path):
            try:
                count = db.execute(f"SELECT COUNT(*) FROM read_csv_auto('{path}')").fetchone()[0]
                stats.append({"layer": "Landing", "name": name, "count": count})
            except Exception as e:
                print(f"Error counting {path}: {e}")

    # 2. Bronze
    bronze_paths = {
        "Online Retail (Bronze)": "medallion/bronze/Online_retail_data",
        "POS Billing (Bronze)": "medallion/bronze/pos_billing_data",
        "Warehouse (Bronze)": "medallion/bronze/warehouse_inventory_data",
        "Shipments (Bronze)": "medallion/bronze/shipments_data"
    }
    for name, path in bronze_paths.items():
        if os.path.exists(path):
            count = db.execute(f"SELECT COUNT(*) FROM read_parquet('{path}/**/*.parquet')").fetchone()[0]
            stats.append({"layer": "Bronze", "name": name, "count": count})

    # 3. Silver
    silver_paths = {
        "Online Retail (Silver)": "medallion/silver/online_retail",
        "POS Billing (Silver)": "medallion/silver/pos_billing",
        "Warehouse (Silver)": "medallion/silver/warehouse_logs",
        "Shipments (Silver)": "medallion/silver/shipments"
    }
    for name, path in silver_paths.items():
        if os.path.exists(path):
            count = db.execute(f"SELECT COUNT(*) FROM read_parquet('{path}/**/*.parquet')").fetchone()[0]
            stats.append({"layer": "Silver", "name": name, "count": count})

    # 4. Gold
    gold_entities = {
        "dim_product": DIM_PRODUCT,
        "dim_customer": DIM_CUSTOMER,
        "dim_date": DIM_DATE,
        "fact_sales": FACT_SALES,
        "fact_inventory": FACT_INVENTORY,
        "fact_shipments": FACT_SHIPMENTS
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
    uvicorn.run(app, host="0.0.0.0", port=8001)
