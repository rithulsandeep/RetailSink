from deltalake import DeltaTable
import os

def explore_delta_changes(path):
    if not os.path.exists(path):
        print(f"Path {path} does not exist.")
        return

    dt = DeltaTable(path)
    print(f"Current Version: {dt.version()}")
    
    # Check history
    print("\n--- History (Last 5 commits) ---")
    history = dt.history(5)
    for commit in history:
        print(f"Version: {commit['version']}, Op: {commit.get('operation', 'Unknown')}")
        # In newer delta-rs, we might access actions?
        # Let's try to get added files for a specific version difference?
        pass

    # How to get added files?
    # We can use dt.get_add_actions(flatten=True)?
    try:
        # This returns a pyarrow table or similar
        files = dt.file_uris() 
        print(f"\nTotal Active Files: {len(files)}")
        # print(files[:2])
    except Exception as e:
        print(f"Error getting files: {e}")

if __name__ == "__main__":
    explore_delta_changes("medallion/bronze/Online_retail_data")
