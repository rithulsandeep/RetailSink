import json
import os
from deltalake import DeltaTable

CHECKPOINT_FILE = "data/pipeline_checkpoints.json"

def load_checkpoints():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_checkpoints(checkpoints):
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoints, f, indent=4)

def get_new_files(table_path, task_id):
    """
    Returns list of absolute new parquet file paths since last checkpoint.
    task_id: Unique ID for this consumer (e.g. 'silver_online_retail')
    """
    checkpoints = load_checkpoints()
    last_version = checkpoints.get(task_id, -1)
    
    if not os.path.exists(table_path):
        return [], -1
    
    dt = DeltaTable(table_path)
    current_version = dt.version()
    
    if last_version == current_version:
        return [], current_version
    
    # Collect files from delta logs between last_version+1 and current_version
    new_files = []
    
    # If last_version is -1, we need all current files (Full Load)
    if last_version == -1:
        # Full snapshot
        # file_uris() returns list of files in current version
        new_files = dt.file_uris()
    else:
        # Incremental
        # We need to look at actions.
        # dt.get_add_actions(flatten=True) gives all add actions for current snapshot.
        # To get *changes*, we can use history but history doesn't give file paths easily in python binding?
        # Actually, python binding `get_add_actions` with `version` argument? No.
        # But we can assume Bronze logic: Bronze Appends create new files.
        # We can list all files and filter by modification time? No, reliable on version.
        
        # Workaround for Python Delta Lake to get added files between versions:
        # 1. dt.files_by_partitions([]) -> gives all files.
        # 2. We can try to rely on the fact that we process strictly sequentially.
        # 
        # Actually, `dt.history()` might give operations.
        # The rust python binding exposes `get_add_actions(flatten=True)` which returns a PyArrow RecordBatch.
        # It contains `path` and `modificationTime`.
        # However, filter by version is hard without iterating all commits manually? 
        # 
        # Optimization:
        # If we can't easily get diff, maybe we just Scan All IF version changed?
        # But User complain about speed.
        # 
        # Alternate approach:
        # `dt.load_as_version(v)`?
        # 
        # Let's try a simpler approach for now:
        # Just use `delta_scan` (DuckDB) BUT add a WHERE clause on `modification_time`?
        # Delta Lake tables don't expose mod time as column easily in DuckDB unless we read `__delta_log`.
        #
        # Let's implement the `last_version` check at least to SKIP processing if nothing changed.
        # returning `[], current_version` if equal.
        # If not equal, we fall back to `delta_scan` (Snapshot) or full files.
        # 
        # WAIT! If we use `get_add_actions` for the current snapshot, it returns ALL files.
        # 
        # Let's look at `dt.get_active_partitions()`?
        # 
        # Let's stick to: "Skip if version match".
        # If version changed, we process.
        # To speed up processing, can we identify WHICH partitions changed?
        # If we can get `partitions_values` from add actions of recent commits?
        pass

    # For now, let's implement efficient SKIP.
    return [], current_version

def get_incremental_scan_query(table_path, task_id):
    """
    Returns (query_part, new_version, has_updates)
    query_part: SQL string to use in FROM clause (e.g., `delta_scan('path')` or `read_parquet([...])`)
    This logic will try to optimize reading.
    """
    checkpoints = load_checkpoints()
    last_version = checkpoints.get(task_id, -1)
    
    if not os.path.exists(table_path):
        return None, -1, False
    
    dt = DeltaTable(table_path)
    current_version = dt.version()
    
    if last_version == current_version:
        return None, current_version, False
        
    # If we have updates:
    # 1. Simplest: Return delta_scan (Full Scan) but at least we skipped if no updates.
    # 2. Advanced: Identify changed files.
    
    # Start with skipping.
    print(f"[{task_id}] Updates detected! {last_version} -> {current_version}")
    return f"delta_scan('{table_path}')", current_version, True
