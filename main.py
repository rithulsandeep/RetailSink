import subprocess
import sys
import os
import argparse

def run_script(script_path, args=None, capture_output=False):
    """Runs a python script as a subprocess."""
    cmd = [sys.executable, script_path]
    if args:
        cmd.extend(args)
    
    print(f"\n>>> Running: {' '.join(cmd)}")
    try:
        if capture_output:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            return result.stdout
        else:
            subprocess.run(cmd, check=True)
            return True
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_path}: {e}")
        if capture_output:
            print(e.stderr)
        return False

def main():
    parser = argparse.ArgumentParser(description="Medallion Pipeline Orchestrator")
    parser.add_argument("action", choices=["simulate", "bronze", "silver", "full-etl"], 
                        help="Action to perform")
    parser.add_argument("--store_id", type=str, default="1", help="Store ID for simulation")
    
    args = parser.parse_args()

    # Paths to scripts relative to root
    SIMULATE_SCRIPT = os.path.join("scripts", "data", "simulated_data.py")
    BRONZE_SCRIPT = os.path.join("scripts", "upgrades", "csv_to_parquet.py")
    SILVER_SCRIPT = os.path.join("scripts", "upgrades", "bronze_to_silver.py")

    if args.action == "simulate":
        print(f"Starting Data Simulation for Store {args.store_id}...")
        run_script(SIMULATE_SCRIPT, ["--store_id", args.store_id])

    elif args.action == "bronze":
        print("Running Bronze Layer Conversion (Landing -> Bronze)...")
        run_script(BRONZE_SCRIPT)

    elif args.action == "silver":
        print("Running Silver Layer Conversion (Bronze -> Silver)...")
        run_script(SILVER_SCRIPT)

    elif args.action == "full-etl":
        print("Running Full ETL Process (Bronze & Silver)...")
        if run_script(BRONZE_SCRIPT):
            run_script(SILVER_SCRIPT)

if __name__ == "__main__":
    main()
