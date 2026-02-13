import os
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

# ----------------------------
# CONFIG
# ----------------------------
NUM_ROWS = 10000
OUTPUT_FILE = "landing/warehouse_inventory_data.csv"
fake = Faker("en_IN")

WAREHOUSES = ["WH-MUMBAI-01", "WH-DELHI-02", "WH-BENGALURU-01", "WH-CHENNAI-03", "WH-KOLKATA-01"]
SUPPLIERS = ["Bharat Logistics", "India Mart", "Reliant Supply Co.", "Apex Distribution", "Gati Shipments"]
ORIGINS = ["Mumbai", "Guangzhou", "Hamburg", "New Delhi", "Pune", "Ho Chi Minh City"]

# ----------------------------
# DATA GENERATOR
# ----------------------------
def generate_warehouse_data(num_rows=10000):
    data = []
    
    # Pre-generate products for consistency across systems
    # Using similar ItemCodes to link with POS data later
    product_pool = [
        {
            "SKU_ID": f"POS_{fake.unique.random_int(1000, 9999)}", 
            "Item_Name": fake.catch_phrase(),
            "Category": random.choice(["Electronics", "Fashion", "Home", "Beauty"]),
            "Unit_Weight": round(random.uniform(0.1, 20.0), 2)
        }
        for _ in range(150)
    ]

    print(f"Generating {num_rows} rows of Warehouse Log data...")

    for i in range(num_rows):
        product = random.choice(product_pool)
        
        # Batch and Log numbers
        batch_no = f"BATCH-{random.randint(2024, 2026)}-{random.randint(100, 999)}"
        log_id = f"LOG-{fake.unique.random_int(100000, 999999)}"

        # Movement Type
        movement = random.choice(["INWARD", "OUTWARD", "ADJUSTMENT"])
        
        # Quantities
        qty_affected = random.randint(10, 500)
        # 1% chance movement logic error (negative inward)
        if movement == "INWARD" and random.random() < 0.01:
            qty_affected = -qty_affected

        # Dates
        arrival_date = fake.date_time_between(start_date="-2y", end_date="now")
        # Inconsistent warehouse date formatting
        if random.random() < 0.15:
            date_str = arrival_date.strftime("%b %d, %Y") # e.g. Feb 13, 2026
        else:
            date_str = arrival_date.strftime("%Y/%m/%d")

        # Silo-specific columns
        row = {
            "LogID": log_id,
            "WarehouseCode": random.choice(WAREHOUSES),
            "SKU_ID": product["SKU_ID"],
            "Item_Name": product["Item_Name"],
            "BatchNo": batch_no,
            "MovementType": movement,
            "Quantity_Change": qty_affected,
            "EventDate": date_str,
            "SupplierName": random.choice(SUPPLIERS),
            "CountryOfOrigin": random.choice(ORIGINS),
            "PackageWeight_kg": product["Unit_Weight"],
            "StorageTemp_C": round(random.uniform(15, 35), 1),
            "BinLocation": f"AISLE-{random.randint(1, 20)}-{random.choice(['A', 'B', 'C'])}{random.randint(1, 100)}"
        }
        
        # 2% chance of missing SKU_ID (Orphan logs)
        if random.random() < 0.02:
            row["SKU_ID"] = None
            
        data.append(row)

    return pd.DataFrame(data)

# ----------------------------
# MAIN
# ----------------------------
if __name__ == "__main__":
    # Ensure landing directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    df = generate_warehouse_data(NUM_ROWS)
    
    # Save to CSV
    df.to_csv(OUTPUT_FILE, index=False)
    
    print(f"Successfully generated {len(df)} rows to {OUTPUT_FILE}")
    print("\nWarehouse Silo Schema Sample:")
    print(df.head())
    
    print("\nData silo Quality Check:")
    print(f"Orphan Logs (Missing SKU_ID): {df['SKU_ID'].isnull().sum()}")
    print(f"Unique Batch Groups: {df['BatchNo'].nunique()}")
    print(f"Sample Inconsistent Date (Feb 13, 2026 style): {df[df['EventDate'].str.contains(',')].shape[0]}")
