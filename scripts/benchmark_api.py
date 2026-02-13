import requests
import time

def benchmark(url, name):
    start = time.time()
    try:
        resp = requests.get(url)
        end = time.time()
        if resp.status_code == 200:
            data = resp.json()
            size = len(data) if isinstance(data, list) else 1
            print(f"{name:15}: {size:4} results, {(end-start)*1000:7.2f}ms")
        else:
            print(f"{name:15}: Error {resp.status_code}, {(end-start)*1000:7.2f}ms")
            print(f"Response: {resp.text[:100]}")
    except Exception as e:
        print(f"{name:15}: Exception: {e}")

if __name__ == "__main__":
    print(f"{'Endpoint':15} | {'Count':5} | {'Latency':10}")
    print("-" * 35)
    urls = [
        ("http://localhost:8001/api/kpi/summary", "Summary"),
        ("http://localhost:8001/api/kpi/revenue-trend", "Rev Trend"),
        ("http://localhost:8001/api/kpi/top-products", "Top Products"),
        ("http://localhost:8001/api/kpi/city-sales", "City Sales"),
        ("http://localhost:8001/api/kpi/operations-metrics", "Ops Metrics"),
        ("http://localhost:8001/api/kpi/customer-insights", "Cust Insights"),
        ("http://localhost:8001/api/kpi/sales-channel", "Channel Dist"),
        ("http://localhost:8001/api/kpi/inventory-status", "Inv Status"),
        ("http://localhost:8001/api/kpi/lineage-stats", "Lineage")
    ]
    
    # Warmup
    requests.get(urls[0][0])
    
    for url, name in urls:
        benchmark(url, name)
