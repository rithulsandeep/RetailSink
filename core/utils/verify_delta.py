import duckdb
import pandas as pd

def verify_delta():
    print("--- Verifying Delta Lake Tables ---")
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    
    tables = {
        "dim_product": "medallion/gold/dim_product",
        "dim_customer": "medallion/gold/dim_customer",
        "fact_sales": "medallion/gold/fact_sales"
    }
    
    for name, path in tables.items():
        try:
            count = con.execute(f"SELECT COUNT(*) FROM delta_scan('{path}')").fetchone()[0]
            print(f"Table '{name}': {count} rows")
        except Exception as e:
            print(f"Error reading '{name}': {e}")
            
    print("\n--- Analytics Sample (Top Products) ---")
    query = """
        SELECT p.product_description, SUM(s.total_amount) as revenue 
        FROM delta_scan('medallion/gold/fact_sales') s 
        JOIN delta_scan('medallion/gold/dim_product') p ON s.product_key = p.product_key 
        GROUP BY p.product_description 
        ORDER BY revenue DESC 
        LIMIT 5
    """
    try:
        df = con.query(query).to_df()
        print(df)
    except Exception as e:
        print(f"Error running analytics query: {e}")
        
    con.close()

if __name__ == "__main__":
    verify_delta()
