import duckdb
path = "medallion/gold/fact_sales/**/*.parquet"
db = duckdb.connect()
df = db.query(f"SELECT * FROM read_parquet('{path}', hive_partitioning = true) LIMIT 1").to_df()
print(df.columns.tolist())
