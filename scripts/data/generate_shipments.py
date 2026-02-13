import duckdb
import random
import os
from datetime import timedelta

def generate_shipments():
    print("--- Generating Synthetic Shipment Data ---")
    
    ONLINE_RETAIL_PATH = 'landing/Online_retail_data.csv'
    OUTPUT_PATH = 'landing/shipments_data.csv'
    
    if not os.path.exists(ONLINE_RETAIL_PATH):
        print(f"Error: {ONLINE_RETAIL_PATH} not found.")
        return

    db = duckdb.connect()
    
    # Get unique invoices from Online Retail
    print("Reading unique invoices...")
    unique_invoices = db.query(f"""
        SELECT 
            InvoiceNo as invoice_id,
            MIN(InvoiceDate) as order_timestamp
        FROM read_csv_auto('{ONLINE_RETAIL_PATH}')
        WHERE InvoiceNo IS NOT NULL AND InvoiceNo NOT LIKE 'C%'
        GROUP BY InvoiceNo
    """).to_df()
    
    cities = ["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Ahmedabad", "Chennai", "Kolkata", "Surat", "Pune", "Jaipur", "Lucknow", "Kanpur", "Nagpur", "Indore", "Thane", "Bhopal", "Visakhapatnam", "Pimpri-Chinchwad", "Patna", "Vadodara"]
    
    def generate_shipment_details(row):
        order_time = row['order_timestamp']
        # Ship within 0-2 days
        ship_time = order_time + timedelta(hours=random.randint(4, 48))
        # Deliver within 2-7 days after shipping
        delivery_time = ship_time + timedelta(days=random.randint(2, 7))
        city = random.choice(cities)
        return pd.Series([ship_time, delivery_time, city, "India"])

    import pandas as pd
    print(f"Generating details for {len(unique_invoices)} invoices...")
    unique_invoices[['ship_timestamp', 'delivery_timestamp', 'city', 'country']] = unique_invoices.apply(generate_shipment_details, axis=1)
    
    unique_invoices.to_csv(OUTPUT_PATH, index=False)
    print(f"Successfully generated {len(unique_invoices)} shipment records to {OUTPUT_PATH}")

if __name__ == "__main__":
    generate_shipments()
