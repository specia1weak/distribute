import pandas as pd
import glob
import os
import re

DATA_DIR = r"d:\pyprojects\distribute\data\new"

def parse_lag(filename):
    # Match "2.5klag" or "1klag"
    match = re.search(r"(\d+(\.\d+)?)klag", filename)
    if match:
        return float(match.group(1)) * 1000  # Convert to ms
    return None

def calibrate_file(filepath, lag_ms):
    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        print(f"Failed to read {filepath}: {e}")
        return

    # Check required columns
    # We need 'sys_ts' and 'window_end' to calculate lag_system in trace files
    # BUT, 'window_end' might be -1 for LATE data.
    # User said: "Find min lag-system... compare with lag+100ms... difference is offset"
    # lag_system = sys_ts - window_end
    # This logic applies to metrics/trace rows where valid window trigger happened.
    
    # CASE 1: Trace files (e.g., 3p2.5klag-all-data-trace_P3_task-0.csv)
    # Columns: task_id,sys_ts,event_ts,log_id,content,status,window_end
    if 'sys_ts' in df.columns and 'window_end' in df.columns:
        # Filter for valid window_end (not -1 or 0)
        valid_df = df[df['window_end'] > 0]
        if valid_df.empty:
            print(f"Skipping {os.path.basename(filepath)}: No valid window data for calibration.")
            return

        # Calculate lag_system
        # Note: sys_ts is trigger time (ms)
        lag_systems = valid_df['sys_ts'] - valid_df['window_end']
        min_lag_sys = lag_systems.min()
        
        target_min = lag_ms + 100
        offset = target_min - min_lag_sys
        
        print(f"File: {os.path.basename(filepath)}")
        print(f"  Lag: {lag_ms}ms")
        print(f"  Min Lag System: {min_lag_sys}ms")
        print(f"  Target Min: {target_min}ms")
        print(f"  Offset: {offset:.2f}ms")
        
        if abs(offset) > 10: # Only correct if offset is significant
            df['sys_ts'] = df['sys_ts'] + offset
            # Also update event_ts? No, user said "clock offset", which affects sys_ts (trigger time recorded by machine).
            # event_ts comes from source payload, presumably correct relative to itself.
            # But wait, if sys_ts is local machine time, and event_ts is from generator.
            # Lag system = sys_ts - window_end. Window end is event time domain.
            # So correcting sys_ts aligns local clock to event time domain.
            
            # Save back using temp file
            temp_path = filepath + ".tmp"
            df.to_csv(temp_path, index=False)
            try:
                os.replace(temp_path, filepath)
                print(f"  ✅ Calibrated and saved.")
            except Exception as e:
                print(f"  ❌ Failed to replace {filepath}: {e}")
                if os.path.exists(temp_path): os.remove(temp_path)
        else:
             print(f"  👌 Offset too small, skipping.")

    # CASE 2: Metrics files (e.g., 3p2.5klag-window_Tumbling_P3_task-0.csv)
    # Columns: task_id,window_end,trigger_ts,count_actual...
    elif 'trigger_ts' in df.columns and 'window_end' in df.columns:
         # Same logic but 'trigger_ts' is the system timestamp
        valid_df = df[df['window_end'] > 0]
        if valid_df.empty:
            return

        lag_systems = valid_df['trigger_ts'] - valid_df['window_end']
        min_lag_sys = lag_systems.min()
        
        target_min = lag_ms + 100
        offset = target_min - min_lag_sys
        
        print(f"File: {os.path.basename(filepath)}")
        print(f"  Lag: {lag_ms}ms")
        print(f"  Min Lag System: {min_lag_sys}ms")
        print(f"  Offset: {offset:.2f}ms")
        
        if abs(offset) > 10:
            df['trigger_ts'] = df['trigger_ts'] + offset
            # Recalculate lag_system column if present?
            # 'lag_system' is in columns: task_id,window_end,trigger_ts,count_actual,count_expected,loss_network,lag_system,current_wm
            if 'lag_system' in df.columns:
                df['lag_system'] = df['trigger_ts'] - df['window_end']
            
            # Save back using temp file
            temp_path = filepath + ".tmp"
            df.to_csv(temp_path, index=False)
            try:
                os.replace(temp_path, filepath)
                print(f"  ✅ Calibrated and saved.")
            except Exception as e:
                print(f"  ❌ Failed to replace {filepath}: {e}")
                if os.path.exists(temp_path): os.remove(temp_path)

def main():
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    for f in files:
        # User specifically mentioned 3p2.5klag but logic implies all.
        # But let's check parse_lag
        lag_ms = parse_lag(os.path.basename(f))
        if lag_ms is not None:
            calibrate_file(f, lag_ms)

if __name__ == "__main__":
    main()
