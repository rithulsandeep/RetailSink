import os
import random
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta

# ----------------------------
# CONFIG
# ----------------------------
NUM_ROWS = 10000
OUTPUT_FILE = "landing/pos_billing_data.csv"
fake = Faker("en_IN")

CITIES = ["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Ahmedabad", "Chennai", "Kolkata", "Pune", "Jaipur", "Lucknow"]
NATIONS = ["India"]

def get_simulated_now():
    factor = float(os.environ.get("SIM_ACCELERATION_FACTOR", 1.0))
    start_real = float(os.environ.get("SIM_START_REAL_TIME", time.time()))
    start_virtual = float(os.environ.get("SIM_START_VIRTUAL_TIME", start_real))
    
    elapsed_real = time.time() - start_real
    simulated_time = start_virtual + (elapsed_real * factor)
    return datetime.fromtimestamp(simulated_time)

# ----------------------------
# DATA GENERATOR
# ----------------------------
def generate_messy_pos_data(num_rows=10000):
    data = []
    
    # Pre-generate some products for consistency
    product_pool = [
        {"ItemCode": f"POS_{fake.unique.random_int(1000, 9999)}", "ProductName": fake.catch_phrase()}
        for _ in range(150)
    ]

    print(f"Generating {num_rows} rows of messy POS billing data...")

    sim_now = get_simulated_now()

    for i in range(num_rows):
        # 3% chance of multi-item bill (duplicate BillNo)
        if i > 0 and random.random() < 0.03:
            bill_no = data[-1]["BillNo"]
        else:
            bill_no = f"POS-{fake.unique.random_int(200000, 899999)}"

        product = random.choice(product_pool)
        
        # 5% chance of missing LoyaltyID
        loyalty_id = fake.random_int(10000, 99999) if random.random() > 0.05 else None
        
        # Synonyms and Realism
        product_name = product["ProductName"]
        # 2% chance of leading/trailing whitespaces
        if random.random() < 0.02:
            product_name = f"  {product_name}  "
        
        # Qty: 1% chance of garbage string, 1% negative
        qty = fake.random_int(1, 12)
        if random.random() < 0.01:
            qty = random.choice(["NA", "?", "UNKNOWN"])
        elif random.random() < 0.01:
            qty = -qty

        # Rate: 1% chance of garbage, 1% negative
        rate = round(random.uniform(49, 4999), 2)
        if random.random() < 0.01:
            rate = random.choice(["MISSING", "null"])
        elif random.random() < 0.01:
            rate = -rate

        buy_price = round(float(rate) * random.uniform(0.4, 0.75), 2) if isinstance(rate, (int, float)) else 0.0
        
        # StoreCity: casing and whitespaces
        city = random.choice(CITIES)
        if random.random() < 0.03:
            city = city.lower() if random.random() < 0.5 else city.upper()
        if random.random() < 0.02:
            city = f" {city}"

        # BillDate: Use simulated now with some jitter (up to 30 mins ago in sim-time)
        jitter = random.randint(0, 1800)
        invoice_date = sim_now - timedelta(seconds=jitter)
        
        if random.random() < 0.1:
            bill_date = invoice_date.strftime("%d-%m-%Y") # Inconsistent format
        else:
            bill_date = invoice_date.strftime("%Y-%m-%d %H:%M:%S")

        # POS-Unique Columns
        payment_modes = ["CASH", "CARD", "UPI", "WALLET"]
        pay_mode = random.choice(payment_modes)
        # 2% chance of missing PayMode
        if random.random() < 0.02: pay_mode = None
        
        cashier_id = f"STAFF_{random.randint(101, 150)}"
        # 1% chance of garbage cashier ID
        if random.random() < 0.01: cashier_id = "ERR_99"

        row = {
            "BillNo": bill_no,
            "ItemCode": product["ItemCode"],
            "ProductName": product_name,
            "Qty": qty,
            "BillDate": bill_date,
            "Rate": rate,
            "LoyaltyID": loyalty_id,
            "Nation": random.choice(NATIONS),
            "StoreCity": city,
            "BuyPrice": buy_price,
            "StockAtHand": random.randint(0, 500),
            "PayMode": pay_mode,
            "CashierID": cashier_id,
            "ShopTaxRate": random.choice([0.05, 0.12, 0.18])
        }
        data.append(row)

    return pd.DataFrame(data)

import time

def simulate_chunk_ingestion(interval_seconds=120):
    print(f"--- Starting POS Billing Chunk Ingestion to {OUTPUT_FILE} (Interval: {interval_seconds}s) ---")
    
    # Ensure landing directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    while True:
        # Generate a chunk of 50-100 rows
        num_rows = random.randint(50, 100)
        df = generate_messy_pos_data(num_rows)
        
        # Save to CSV (append mode)
        header = not os.path.exists(OUTPUT_FILE)
        df.to_csv(OUTPUT_FILE, mode='a', index=False, header=header)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Ingested chunk of {num_rows} POS billing records.")
        
        # Sleep for the interval (scaled by acceleration factor)
        factor = float(os.environ.get("SIM_ACCELERATION_FACTOR", 1.0))
        time.sleep(max(0.1, interval_seconds / factor))

# ----------------------------
# MAIN
# ----------------------------
if __name__ == "__main__":
    try:
        # Initial generation if file doesn't exist? 
        # Or just start the loop. Let's just start the loop.
        simulate_chunk_ingestion(600) # 10 minutes
    except KeyboardInterrupt:
        print("\nStopping simulation.")


