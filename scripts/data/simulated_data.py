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

        # BillDate: Inconsistent formats
        invoice_date = fake.date_time_between(start_date="-2y", end_date="now")
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

# ----------------------------
# MAIN
# ----------------------------
if __name__ == "__main__":
    # Ensure landing directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    df = generate_messy_pos_data(NUM_ROWS)
    
    # Save to CSV
    df.to_csv(OUTPUT_FILE, index=False)
    
    print(f"Successfully generated {len(df)} rows to {OUTPUT_FILE}")
    print("\nRefined Schema Sample (Synonyms):")
    print(df.head())
    
    print("\nMessiness Check:")
    print(f"Nulls in LoyaltyID: {df['LoyaltyID'].isnull().sum()}")
    print(f"Unique Dates Formats: {df['BillDate'].apply(lambda x: '-' in x).value_counts()}")
    print(f"Garbage strings in Qty: {df[df['Qty'].apply(lambda x: isinstance(x, str))]['Qty'].unique()}")


