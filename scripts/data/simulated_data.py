import argparse
import time
import random
import pandas as pd
from faker import Faker
from datetime import datetime

# ----------------------------
# ARGUMENTS
# ----------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--store_id", type=int, required=True)
args = parser.parse_args()

STORE_ID = args.store_id

fake = Faker("en_IN")

# ----------------------------
# CONFIG
# ----------------------------
OUTPUT_FILE = "landing/live_sales_data.csv"

CATEGORIES = ["Electronics", "Fashion", "Groceries", "Home", "Beauty"]
CHANNELS = ["Store", "Online"]
PAYMENT_METHODS = ["Card", "UPI", "Cash", "NetBanking"]
REGIONS = {
    "Mumbai": "West",
    "Pune": "West",
    "Delhi": "North",
    "Noida": "North",
    "Bengaluru": "South",
    "Chennai": "South",
    "Kolkata": "East"
}

# Simulated loyal customers pool
CUSTOMER_POOL = list(range(1, 501))

# ----------------------------
# BEHAVIOR ENGINE
# ----------------------------
def calculate_quantity(base_quantity, timestamp, channel):
    multiplier = 1.0
    weekday = timestamp.weekday()
    hour = timestamp.hour

    # Weekend boost
    if weekday >= 5:
        multiplier *= 1.4

    # Store evening rush
    if channel == "Store" and 17 <= hour <= 21:
        multiplier *= 1.3

    # Online late-night spike
    if channel == "Online" and 21 <= hour <= 23:
        multiplier *= 1.5

    return max(1, int(base_quantity * multiplier))


def is_holiday(timestamp):
    # Simple example: treat Sundays as holiday
    return 1 if timestamp.weekday() == 6 else 0


def generate_discount(category, channel):
    if category == "Fashion":
        return round(random.uniform(0.05, 0.30), 2)
    if channel == "Online":
        return round(random.uniform(0.00, 0.15), 2)
    return round(random.uniform(0.00, 0.10), 2)


# ----------------------------
# ORDER GENERATOR
# ----------------------------
def generate_order():
    now = datetime.now()

    channel = random.choice(CHANNELS)
    category = random.choice(CATEGORIES)

    base_quantity = random.randint(1, 3)
    quantity = calculate_quantity(base_quantity, now, channel)

    price_per_unit = round(random.uniform(100, 5000), 2)
    discount = generate_discount(category, channel)

    total_amount = round(quantity * price_per_unit * (1 - discount), 2)

    city = random.choice(list(REGIONS.keys()))
    region = REGIONS[city]

    customer_id = random.choice(CUSTOMER_POOL)  # repeat customers

    # 5% chance of cancellation
    status = "Completed"
    if random.random() < 0.05:
        status = "Cancelled"
        total_amount = 0

    return {
        "order_id": random.randint(100000, 999999),
        "store_id": STORE_ID,
        "customer_id": customer_id,
        "customer_city": city,
        "region": region,
        "product_category": category,
        "channel": channel,
        "quantity": quantity,
        "price_per_unit": price_per_unit,
        "discount": discount,
        "payment_method": random.choice(PAYMENT_METHODS),
        "holiday_flag": is_holiday(now),
        "order_status": status,
        "total_amount": total_amount,
        "timestamp": now
    }


# ----------------------------
# STREAM LOOP
# ----------------------------
print(f"Starting enhanced live stream for Store {STORE_ID}...")

while True:
    order = generate_order()
    df = pd.DataFrame([order])

    # Ensure landing directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    try:
        df.to_csv(OUTPUT_FILE, mode="a", header=False, index=False)
    except FileNotFoundError:
        df.to_csv(OUTPUT_FILE, mode="w", header=True, index=False)

    print(f"Store {STORE_ID} | New Order:", order)

    time.sleep(random.randint(2, 5))
