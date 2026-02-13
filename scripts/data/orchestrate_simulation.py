import subprocess
import os
import sys
import time
import argparse
from datetime import datetime

def run_simulators():
    parser = argparse.ArgumentParser(description="Orchestrate data simulators with temporal acceleration.")
    parser.add_argument("--factor", type=float, default=1.0, help="Acceleration factor (e.g., 1440 for 1 day per minute)")
    args = parser.parse_args()

    scripts = [
        "scripts/data/online_retail_data.py",
        "scripts/data/simulated_data.py",
        "scripts/data/warehouse_data.py",
        "scripts/data/generate_shipments.py"
    ]
    
    processes = []
    
    # Synchronization environment variables
    env = os.environ.copy()
    env["SIM_ACCELERATION_FACTOR"] = str(args.factor)
    env["SIM_START_REAL_TIME"] = str(time.time())
    env["SIM_START_VIRTUAL_TIME"] = str(time.time()) # Can be customized to start at a specific date

    print(f"--- Starting All Data Simulators (Acceleration Factor: {args.factor}x) ---")
    if args.factor > 1:
        print(f"Simulation speed: 1 real second = {args.factor} simulated seconds")
        print(f"                  1 real minute = {args.factor/60:.2f} simulated hours")
    
    for script in scripts:
        full_path = os.path.abspath(script)
        if not os.path.exists(full_path):
            print(f"Error: Script {script} not found at {full_path}")
            continue
            
        print(f"Starting {script}...")
        # Pass environment variables to sub-processes
        p = subprocess.Popen([sys.executable, full_path], env=env)
        processes.append(p)
    
    print("\nAll simulators are running. Press Ctrl+C to stop all.\n")
    
    try:
        while True:
            # Check if any process has terminated
            for p in processes:
                if p.poll() is not None:
                    print(f"Warning: One of the simulators (PID {p.pid}) has stopped.")
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nStopping all simulators...")
        for p in processes:
            p.terminate()
        print("All simulators stopped.")

if __name__ == "__main__":
    run_simulators()
