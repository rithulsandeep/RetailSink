import duckdb
import os

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

gold_root = 'medallion/gold'
dim_customer = os.path.join(gold_root, 'dim_customer')
fact_sales = os.path.join(gold_root, 'fact_sales')

print("--- Verifying dim_customer SCD Type 2 ---")
cust_history = con.execute(f"SELECT customer_id, count(*) as versions FROM delta_scan('{dim_customer}') GROUP BY customer_id HAVING count(*) > 1").fetchall()

if cust_history:
    print(f"Found {len(cust_history)} customers with history:")
    for cid, vcount in cust_history[:5]:
        print(f"Customer {cid}: {vcount} versions")
        history = con.execute(f"SELECT customer_key, city, country, valid_from, valid_to, is_current FROM delta_scan('{dim_customer}') WHERE customer_id = '{cid}' ORDER BY valid_from").fetchall()
        for row in history:
            print(f"  Version {row[0]}: {row[1]}, {row[2]} ({row[3]} to {row[4]}) Current: {row[5]}")
else:
    print("No customers found with multiple versions in this dataset (possibly no address changes in original data).")

print("\n--- Verifying fact_sales Joins ---")
# Check if any sales have customer_key 0 (Unknown) when they should have been matched
unknown_sales = con.execute(f"SELECT count(*) FROM delta_scan('{fact_sales}') WHERE customer_key = 0").fetchone()[0]
total_sales = con.execute(f"SELECT count(*) FROM delta_scan('{fact_sales}')").fetchone()[0]
print(f"Total Sales: {total_sales}")
print(f"Sales with Unknown Customer: {unknown_sales} ({unknown_sales/total_sales*100:.2f}%)")

# Check if customer_key in fact_sales actually exists in dim_customer
invalid_keys = con.execute(f"""
    SELECT count(*) 
    FROM delta_scan('{fact_sales}') f
    LEFT JOIN delta_scan('{dim_customer}') d ON f.customer_key = d.customer_key
    WHERE d.customer_key IS NULL
""").fetchone()[0]
print(f"Sales with invalid customer_key: {invalid_keys}")

con.close()
