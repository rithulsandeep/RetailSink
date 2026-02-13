import os
import time
import json
import subprocess
import sys
from datetime import datetime

# --- TEMPORAL CONFIGURATION ---
TEMPORAL_ACCELERATION = 1  # 1 real minute = 1 simulated day (24h)
STATE_FILE = "data/state.json"
# ------------------------------

# --- PIPELINE CONFIGURATION ---
PIPELINE_INTERVAL_LIVE = 60    # Landing -> Bronze -> Silver every 1 real minute
PIPELINE_INTERVAL_CHUNK = 300  # Silver -> Gold every 5 real minutes
# ------------------------------

def load_state():
    """Loads the last recorded virtual time."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                data = json.load(f)
                return data.get("last_virtual_time", time.time())
        except:
            pass
    return time.time()

def save_state(virtual_time):
    """Saves the current virtual time to ensure no 'time travel' on restart."""
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump({"last_virtual_time": virtual_time, "updated_at": datetime.now().isoformat()}, f, indent=4)

def run_step(name, script_path, env):
    """Runs a single pipeline step."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Running {name}...")
    try:
        subprocess.run([sys.executable, script_path], env=env, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error in {name}: {e}")
        return False

def main():
    # 1. Initialize Temporal State
    virtual_start = load_state()
    real_start = time.time()
    
    print("=" * 60)
    print("       RETAILSINK MASTER ORCHESTRATION ENGINE")
    print("=" * 60)
    print(f"Virtual Start Time : {datetime.fromtimestamp(virtual_start)}")
    print(f"Acceleration Factor: {TEMPORAL_ACCELERATION}x")
    print(f"Live Flow Interval : {PIPELINE_INTERVAL_LIVE}s")
    print(f"Chunk Flow Interval: {PIPELINE_INTERVAL_CHUNK}s")
    print("-" * 60)

    # Prepare environment for subprocesses
    env = os.environ.copy()
    env["SIM_ACCELERATION_FACTOR"] = str(TEMPORAL_ACCELERATION)
    env["SIM_START_REAL_TIME"] = str(real_start)
    env["SIM_START_VIRTUAL_TIME"] = str(virtual_start)
    env["PYTHONPATH"] = os.getcwd()

    # 2. Start Background Services (Simulators & API)
    services = {
        "POS Simulator": "core/simulators/pos_simulator.py",
        "Shipment Simulator": "core/simulators/shipment_simulator.py",
        "Inventory Simulator": "core/simulators/inventory_simulator.py",
        "Web Simulator": "core/simulators/web_simulator.py",
        "FastAPI Server": "api/main.py"
    }
    
    processes = []
    for name, path in services.items():
        print(f"Starting {name} background process...")
        p = subprocess.Popen([sys.executable, path], env=env)
        processes.append(p)

    print("-" * 60)
    print("Platform is now RUNNING.")
    print("To start the Dashboard UI, run: cd ui && npm run dev")
    print("Press Ctrl+C to stop the entire platform.")
    print("=" * 60)

    last_live_run = 0
    last_chunk_run = 0

    try:
        while True:
            now = time.time()
            # Calculate current virtual time
            current_virtual = virtual_start + (now - real_start) * TEMPORAL_ACCELERATION
            save_state(current_virtual)

            # Check for LIVE flow (Landing -> Silver)
            if now - last_live_run >= PIPELINE_INTERVAL_LIVE:
                print(f"\n[{datetime.fromtimestamp(current_virtual).strftime('%Y-%m-%d %H:%M:%S')}] Triggering LIVE Flow...")
                if run_step("Bronze Layer", "pipeline/bronze_layer.py", env):
                    run_step("Silver Layer", "pipeline/silver_layer.py", env)
                last_live_run = now

            # Check for CHUNK flow (Silver -> Gold)
            if now - last_chunk_run >= PIPELINE_INTERVAL_CHUNK:
                print(f"\n[{datetime.fromtimestamp(current_virtual).strftime('%Y-%m-%d %H:%M:%S')}] Triggering CHUNK Flow...")
                run_step("Gold Layer", "pipeline/gold_layer.py", env)
                last_chunk_run = now

            # Check if any background process died
            for p in processes:
                if p.poll() is not None:
                    print(f"WARNING: One of the background processes died (PID: {p.pid})")
            
            time.sleep(2) # State update frequency

    except KeyboardInterrupt:
        print("\nShutting down RetailSink...")
        for p in processes:
            p.terminate()
        print("Shutdown complete.")

if __name__ == "__main__":
    main()
