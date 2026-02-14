import duckdb
import random
import os
import time
from datetime import datetime, timedelta

def get_simulated_now():
    factor = float(os.environ.get("SIM_ACCELERATION_FACTOR", 1.0))
    start_real = float(os.environ.get("SIM_START_REAL_TIME", time.time()))
    start_virtual = float(os.environ.get("SIM_START_VIRTUAL_TIME", start_real))
    
    elapsed_real = time.time() - start_real
    simulated_time = start_virtual + (elapsed_real * factor)
    return datetime.fromtimestamp(simulated_time)

def simulate_shipment_generation(interval_seconds=60):
    print(f"--- Starting Incremental Shipment Generation (Interval: {interval_seconds}s) ---")
    
    ONLINE_RETAIL_PATH = 'landing/Online_retail_data.csv'
    OUTPUT_PATH = 'landing/shipments_data.csv'
    
    db = duckdb.connect()
    
    while True:
        if not os.path.exists(ONLINE_RETAIL_PATH):
            print(f"Waiting for {ONLINE_RETAIL_PATH}...")
            time.sleep(10)
            continue

        # Find invoices that don't have shipments yet
        shipments_exists = os.path.exists(OUTPUT_PATH)
        
        if shipments_exists:
            query = f"""
                SELECT 
                    r.InvoiceNo as invoice_id,
                    MIN(r.InvoiceDate) as order_timestamp
                FROM read_csv_auto('{ONLINE_RETAIL_PATH}') r
                LEFT JOIN (SELECT DISTINCT invoice_id FROM read_csv_auto('{OUTPUT_PATH}')) s
                    ON CAST(r.InvoiceNo AS VARCHAR) = CAST(s.invoice_id AS VARCHAR)
                WHERE r.InvoiceNo IS NOT NULL 
                  AND CAST(r.InvoiceNo AS VARCHAR) NOT LIKE 'C%'
                  AND s.invoice_id IS NULL
                GROUP BY r.InvoiceNo
            """
        else:
            query = f"""
                SELECT 
                    InvoiceNo as invoice_id,
                    MIN(InvoiceDate) as order_timestamp
                FROM read_csv_auto('{ONLINE_RETAIL_PATH}')
                WHERE InvoiceNo IS NOT NULL AND CAST(InvoiceNo AS VARCHAR) NOT LIKE 'C%'
                GROUP BY InvoiceNo
            """

        try:
            new_invoices = db.query(query).to_df()
        except Exception as e:
            print(f"Error querying data: {e}")
            time.sleep(10)
            continue

        if not new_invoices.empty:
            import pandas as pd
            sim_now = get_simulated_now()
            print(f"[{sim_now.strftime('%H:%M:%S')}] Generating shipments for {len(new_invoices)} new invoices...")
            
            destination_countries = ["India", "United Kingdom", "France", "Germany", "USA", "Australia", "UAE", "Japan"]
            cities = ["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Ahmedabad", "Chennai", "Kolkata", "Surat", "Pune", "Jaipur", "Lucknow", "Kanpur", "Nagpur", "Indore", "Thane", "Bhopal", "Visakhapatnam", "Pimpri-Chinchwad", "Patna", "Vadodara"]
            
            def generate_shipment_details(row):
                order_time = pd.to_datetime(row['order_timestamp'])
                ship_time = order_time + timedelta(hours=random.randint(4, 48))
                delivery_time = ship_time + timedelta(days=random.randint(2, 7))
                city = random.choice(cities)
                country = random.choice(destination_countries)
                return pd.Series([ship_time, delivery_time, city, country])

            new_invoices[['ship_timestamp', 'delivery_timestamp', 'city', 'country']] = new_invoices.apply(generate_shipment_details, axis=1)
            
            # Append to CSV
            header = not shipments_exists
            new_invoices.to_csv(OUTPUT_PATH, mode='a', index=False, header=header)
            print(f"Successfully added {len(new_invoices)} shipment records.")
            
        # Sleep for the interval (scaled by acceleration factor)
        factor = float(os.environ.get("SIM_ACCELERATION_FACTOR", 1.0))
        time.sleep(max(0.1, interval_seconds / factor))

if __name__ == "__main__":
    try:
        simulate_shipment_generation(300) # 5 minutes
    except KeyboardInterrupt:
        print("\nStopping shipment simulation.")
