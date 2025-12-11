
import os
import glob
import re
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# --- Matplotlib 中文支持 ---
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'WenQuanYi Micro Hei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# --- Configuration ---
DATA_ROOT = r'd:\pyprojects\distribute\data'
SINGLE_DIR = os.path.join(DATA_ROOT, 'single')
DIST_DIR = os.path.join(DATA_ROOT, 'dist')
PLOTS_DIR = os.path.join(r'd:\pyprojects\distribute\src', 'plots', 'advanced')
os.makedirs(PLOTS_DIR, exist_ok=True)

DEFAULT_EXCLUDE_FIRST = 10
DEFAULT_EXCLUDE_LAST = 20

# 复用 analyze_data.py 的正则
FILE_PATTERN = re.compile(r'(.+)-experiment_(late_data|window_stats)_slide_SZ(\d+)-TP(\d+)-P(\d+)\.csv')

def load_data():
    """Load and organize all experimental data."""
    experiments = {}

    # 1. Load Single Data
    # 2. Load Dist Data (merged by experiment name)
    # ... (Logic similar to analyze_data.py but keeping raw dataframes for detailed analysis)
    
    # helper to process file list
    def process_files(file_list, mode):
        for file_path in file_list:
            filename = os.path.basename(file_path)
            match = FILE_PATTERN.match(filename)
            if match:
                exp_name, file_type, sz, tp, p = match.groups()
                
                # For Dist, remove '3Dist-' prefix to group properly if needed, 
                # but here we keep full name to distinguish 'remake'
                key = f"{mode}_{exp_name}"
                
                if key not in experiments:
                    experiments[key] = {
                        'mode': mode,
                        'original_name': exp_name,
                        'key': key,
                        'lag': 0, # Placeholder, parse later
                        'late_files': [],
                        'stats_files': []
                    }
                
                if file_type == 'late_data':
                    experiments[key]['late_files'].append(file_path)
                else:
                    experiments[key]['stats_files'].append(file_path)

    print("Loading Single Data...")
    process_files(glob.glob(os.path.join(SINGLE_DIR, "*.csv")), 'Single')
    
    print("Loading Dist Data...")
    # Recursively find all csv in dist hosts
    process_files(glob.glob(os.path.join(DIST_DIR, "**", "*.csv"), recursive=True), 'Dist')
    
    return experiments

def parse_lag_from_name(name):
    # Quick parser (same as analyze_data.py)
    name = name.lower()
    match = re.search(r'([\d\.]+)k?[-_]?lag', name)
    if match:
        val_str = match.group(1)
        is_k = 'k' in match.group(0)
        try:
            val = float(val_str)
            return int(val * 1000) if is_k else int(val)
        except:
            return -1
    return -1

def analyze_host_load_balance(stats_files):
    """
    Analyze if tasks are balanced across hosts (TaskManagers).
    Returns a DataFrame with host_id (task_id), count, avg_latency.
    """
    dfs = []
    for f in stats_files:
        try:
            # Check if file has 'task_id' column (first column)
            # Schema: task_id,window_end,trigger_ts,count_actual,count_expected,loss_network,lag_system,watermark_setting
            df = pd.read_csv(f, on_bad_lines='skip')
            if not df.empty and 'task_id' in df.columns:
                host_folder = os.path.basename(os.path.dirname(f))
                df['host'] = host_folder # e.g., 'host1'
                dfs.append(df)
        except:
            pass
            
    if not dfs:
        return None
        
    merged = pd.concat(dfs, ignore_index=True)
    
    # Group by Host
    balance = merged.groupby('host').agg(
        window_count=('window_end', 'count'),
        avg_latency=('lag_system', 'mean'),
        p99_latency=('lag_system', lambda x: np.percentile(x, 99) if len(x)>0 else 0)
    ).reset_index()
    
    return balance

def plot_advanced_analysis(key, info, exclude_first, exclude_last):
    """
    Generate a dashboard for a single experiment.
    """
    print(f"Generating advanced plots for {key}...")
    
    # 1. Load Data
    late_dfs = []
    for f in info['late_files']:
        try:
            df = pd.read_csv(f, on_bad_lines='skip')
            if not df.empty: late_dfs.append(df)
        except: pass
        
    stats_dfs = []
    host_counts = {} # Debug info
    
    for f in info['stats_files']:
        try:
            df = pd.read_csv(f, on_bad_lines='skip')
            if not df.empty: 
                # 标记 Host 信息: data/dist/host1/xxx.csv -> host1
                host_name = os.path.basename(os.path.dirname(f))
                df['host'] = host_name
                stats_dfs.append(df)
                host_counts[host_name] = host_counts.get(host_name, 0) + len(df)
        except: pass
        
    df_late = pd.concat(late_dfs, ignore_index=True) if late_dfs else pd.DataFrame()
    df_stats = pd.concat(stats_dfs, ignore_index=True) if stats_dfs else pd.DataFrame()
    
    if df_stats.empty:
        print(f"  No stats data for {key}, skipping.")
        return

    # Debug print
    print(f"  Data Loaded for {key}: {host_counts}")

    # 2. Filter Time (Exclude First/Last)
    if 'window_end' in df_stats.columns:
        min_ts = df_stats['window_end'].min()
        max_ts = df_stats['window_end'].max()
        start_cutoff = min_ts + exclude_first * 1000
        end_cutoff = max_ts - exclude_last * 1000
        
        # 记录过滤前数量
        raw_len = len(df_stats)
        df_stats = df_stats[(df_stats['window_end'] >= start_cutoff) & (df_stats['window_end'] <= end_cutoff)]
        print(f"  Filtered {raw_len - len(df_stats)} rows (Time Cutoff). Remaining: {len(df_stats)}")
    
    if df_stats.empty:
        print(f"  No data after filtering for {key}, skipping.")
        return

    # 3. Create Dashboard
    sns.set_theme(style="whitegrid", palette="deep")
    fig = plt.figure(figsize=(16, 12)) # Slightly smaller since less complexity
    gs = fig.add_gridspec(2, 2)
    
    title_str = f"Analysis: {info['original_name']} ({info['mode']})\n"
    title_str += f"Total Samples: {len(df_stats)}"
    fig.suptitle(title_str, fontsize=16, y=0.98)
    
    # 归一化时间
    base_time = df_stats['window_end'].min()
    df_stats['rel_time'] = (df_stats['window_end'] - base_time) / 1000
    
    # --- Plot 1: Overall Latency Distribution (Violin + Box) ---
    ax1 = fig.add_subplot(gs[0, 0])
    # Violin plot shows density better than boxplot alone
    sns.violinplot(y=df_stats['lag_system'], ax=ax1, color='skyblue', inner='quartile')
    ax1.set_title(f'整体延迟分布 (Avg: {df_stats["lag_system"].mean():.0f}ms)', fontsize=14)
    ax1.set_ylabel('System Latency (ms)')
    
    # --- Plot 2: Latency Timeline (Line + Scatter) ---
    ax2 = fig.add_subplot(gs[0, 1])
    # Plot all points to show variance at each timestamp
    sns.scatterplot(data=df_stats, x='rel_time', y='lag_system', color='royalblue', alpha=0.4, s=30, ax=ax2)
    # Add a smooth trend line (rolling mean)
    df_sorted = df_stats.sort_values('rel_time')
    df_sorted['rolling_avg'] = df_sorted['lag_system'].rolling(window=10, center=True).mean()
    sns.lineplot(data=df_sorted, x='rel_time', y='rolling_avg', color='darkblue', ax=ax2, label='Moving Avg (10)')
    
    ax2.set_title('整体延迟时间线 (Latency Timeline)', fontsize=14)
    ax2.set_xlabel('Experiment Time (s)')
    ax2.set_ylabel('Latency (ms)')
    ax2.legend()
    
    # --- Plot 3: Drop Distribution (Histogram) ---
    ax3 = fig.add_subplot(gs[1, 0])
    if not df_late.empty and 'event_ts' in df_late.columns:
        base_late = df_late['event_ts'].min()
        df_late['rel_time'] = (df_late['event_ts'] - base_late) / 1000 
        
        sns.histplot(data=df_late, x='rel_time', bins=50, color='crimson', kde=True, ax=ax3)
        ax3.set_title(f'丢包时间分布 (Total Drops: {len(df_late)})', fontsize=14)
        ax3.set_xlabel('Event Time (s)')
        ax3.set_ylabel('Drop Count')
    else:
        ax3.text(0.5, 0.5, "No Late Data (0 Drops) ✅", ha='center', va='center', fontsize=16)
        ax3.set_title('丢包统计')
        
    # --- Plot 4: Watermark Smoothness (Trigger Intervals) ---
    ax4 = fig.add_subplot(gs[1, 1])
    sorted_triggers = np.sort(df_stats['trigger_ts'].unique())
    diffs = np.diff(sorted_triggers)
    valid_diffs = diffs[diffs < 5000] 
    
    sns.histplot(valid_diffs, ax=ax4, color='green', kde=True, stat='density')
    ax4.set_title('Watermark 推进平滑度 (Trigger Intervals)', fontsize=14)
    ax4.set_xlabel('Trigger Interval (ms)')
    ax4.set_ylabel('Density')
    
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    save_path = os.path.join(PLOTS_DIR, f"{key}_overall_dashboard.png")
    plt.savefig(save_path, dpi=150)
    print(f"  Saved {save_path}")
    plt.close()

def main():
    parser = argparse.ArgumentParser(description='Advanced Flink Analysis')
    parser.add_argument('--exclude-first', type=int, default=DEFAULT_EXCLUDE_FIRST)
    parser.add_argument('--exclude-last', type=int, default=DEFAULT_EXCLUDE_LAST)
    parser.add_argument('--target', type=str, help='Specific experiment key to analyze (partial match)')
    args = parser.parse_args()
    
    experiments = load_data()
    
    for key, info in experiments.items():
        # Optional: Filter specific targets
        if args.target and args.target not in key:
            continue
            
        print(f"Processing {key}...")
        
        # 只分析有数据的实验
        if parse_lag_from_name(info['original_name']) >= 0:
            plot_advanced_analysis(key, info, args.exclude_first, args.exclude_last)

if __name__ == "__main__":
    main()
