import os
import random
import pandas as pd
from faker import Faker
from datetime import datetime
import time

# ----------------------------
# CONFIG
# ----------------------------
OUTPUT_FILE = "landing/Online_retail_data.csv"
fake = Faker("en_IN")

CITIES = ["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Ahmedabad", "Chennai", "Kolkata", "Surat", "Pune", "Jaipur"]

def get_simulated_now():
    factor = float(os.environ.get("SIM_ACCELERATION_FACTOR", 1.0))
    start_real = float(os.environ.get("SIM_START_REAL_TIME", time.time()))
    start_virtual = float(os.environ.get("SIM_START_VIRTUAL_TIME", start_real))
    
    elapsed_real = time.time() - start_real
    simulated_time = start_virtual + (elapsed_real * factor)
    return datetime.fromtimestamp(simulated_time)

def generate_online_retail_row():
    invoice_no = f"{random.randint(536365, 581587)}"
    if random.random() < 0.1: # 10% chance of cancellation
        invoice_no = f"C{invoice_no}"
        
    sim_now = get_simulated_now()
    return {
        "InvoiceNo": invoice_no,
        "StockCode": f"{random.randint(10000, 99999)}{random.choice(['', 'A', 'B', 'C'])}",
        "Description": fake.catch_phrase(),
        "Quantity": random.randint(1, 50),
        "InvoiceDate": sim_now.strftime("%Y-%m-%d %H:%M:%S"),
        "UnitPrice": round(random.uniform(0.5, 20.0), 2),
        "CustomerID": random.randint(12344, 18287),
        "Country": "United Kingdom",
        "City": random.choice(CITIES),
        "Cost_Price": round(random.uniform(0.1, 10.0), 2),
        "Initial_Stock_Level": random.randint(100, 1000),
        "Delivery_Date": (sim_now).strftime("%Y-%m-%d %H:%M:%S")
    }

def simulate_live_ingestion():
    print(f"--- Starting Live Online Retail Ingestion to {OUTPUT_FILE} ---")
    
    # Ensure file exists with headers if it doesn't
    if not os.path.exists(OUTPUT_FILE):
        df = pd.DataFrame([generate_online_retail_row()])
        df.to_csv(OUTPUT_FILE, index=False)
    
    while True:
        num_records = random.randint(1, 5)
        new_records = [generate_online_retail_row() for _ in range(num_records)]
        df = pd.DataFrame(new_records)
        
        # Append to CSV
        df.to_csv(OUTPUT_FILE, mode='a', header=False, index=False)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Ingested {num_records} new online retail records.")
        
        # Sleep for 10-30 seconds
        time.sleep(random.randint(10, 30))

if __name__ == "__main__":
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    try:
        simulate_live_ingestion()
    except KeyboardInterrupt:
        print("\nStopping simulation.")
