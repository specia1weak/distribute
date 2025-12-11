
import os
import glob
import re
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np

# --- Matplotlib 中文支持 ---
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'WenQuanYi Micro Hei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

# --- Configuration ---
DATA_ROOT = r'd:\pyprojects\distribute\data'
SINGLE_DIR = os.path.join(DATA_ROOT, 'single')
DIST_DIR = os.path.join(DATA_ROOT, 'dist')
PLOTS_DIR = os.path.join(r'd:\pyprojects\distribute\src', 'plots')
DEFAULT_TOTAL_LOGS = 20000

# 默认参数（可通过命令行覆盖）
DEFAULT_EXCLUDE_FIRST = 10  # 排除开头 N 秒（启动效应）
DEFAULT_EXCLUDE_LAST = 20   # 排除最后 N 秒（Watermark停滞）
PREFER_REMAKE = True        # 优先使用 remake 版本

os.makedirs(PLOTS_DIR, exist_ok=True)

# --- Regex for Filename Parsing ---
# Matches: "3Dist-1klag", "window_stats", "SZ2000", "TP2000", "P3"
FILE_PATTERN = re.compile(r'(.+)-experiment_(late_data|window_stats)_slide_SZ(\d+)-TP(\d+)-P(\d+)\.csv')


def parse_lag_from_name(name):
    """
    Extracts lag in milliseconds from experiment name.
    Examples:
    '0-lag' -> 0
    '1k-lag' -> 1000
    '2.5k-lag' -> 2500
    '500-lag' -> 500
    '3Dist-1klag' -> 1000
    """
    name = name.lower()
    # Find patterns like '1k', '2.5k', '500' before 'lag'
    match = re.search(r'([\d\.]+)k?[-_]?lag', name)
    if match:
        val_str = match.group(1)
        is_k = 'k' in match.group(0)
        try:
            val = float(val_str)
            if is_k:
                return int(val * 1000)
            else:
                return int(val)
        except ValueError:
            pass
            
    # Fallback for '0-lag' or '0klag'
    if '0-lag' in name or '0klag' in name:
        return 0
        
    return -1 # Unknown

def load_data():
    experiments = {}

    # --- 1. Load Single Data ---
    print("Loading Single Data...")
    for file_path in glob.glob(os.path.join(SINGLE_DIR, '*.csv')):
        filename = os.path.basename(file_path)
        match = FILE_PATTERN.match(filename)
        if match:
            exp_name, file_type, sz, tp, p = match.groups()
            
            # Key for grouping
            key = f"Single_{exp_name}"
            
            if key not in experiments:
                experiments[key] = {
                    'mode': 'Single',
                    'original_name': exp_name,
                    'lag': parse_lag_from_name(exp_name),
                    'sz': int(sz),
                    'tp': int(tp),
                    'p': int(p),
                    'late_files': [],
                    'stats_files': []
                }
            
            if file_type == 'late_data':
                experiments[key]['late_files'].append(file_path)
            else:
                experiments[key]['stats_files'].append(file_path)

    # --- 2. Load Dist Data ---
    print("Loading Dist Data...")
    # Walk through host dirs
    for host_dir in glob.glob(os.path.join(DIST_DIR, 'host*')):
        for file_path in glob.glob(os.path.join(host_dir, '*.csv')):
            filename = os.path.basename(file_path)
            match = FILE_PATTERN.match(filename)
            if match:
                exp_name, file_type, sz, tp, p = match.groups()
                
                # Key for grouping (ignore host, merge by experiment name)
                key = f"Dist_{exp_name}"
                
                if key not in experiments:
                    experiments[key] = {
                        'mode': 'Dist',
                        'original_name': exp_name,
                        'lag': parse_lag_from_name(exp_name),
                        'sz': int(sz),
                        'tp': int(tp),
                        'p': int(p),
                        'late_files': [],
                        'stats_files': []
                    }
                
                if file_type == 'late_data':
                    experiments[key]['late_files'].append(file_path)
                else:
                    experiments[key]['stats_files'].append(file_path)
                    
    return experiments

def analyze_experiments(experiments, exclude_first=10, exclude_last=20):
    results = []
    
    for key, info in experiments.items():
        print(f"Analyzing {key}...")
        
        # 1. Late Data (Drop Count)
        drop_count = 0
        for lf in info['late_files']:
            try:
                # Assuming no header in some files or specific header format. 
                # Checking snippet: "task_id,system_ts,event_ts,log_id,content,lag_magnitude"
                # Some files might be empty or just header
                df = pd.read_csv(lf, on_bad_lines='skip')
                if not df.empty:
                    drop_count += len(df)
            except Exception as e:
                print(f"  Error reading {lf}: {e}")

        # 2. Window Stats (Latency)
        # 收集所有窗口数据，然后按时间过滤
        all_window_data = []
        for sf in info['stats_files']:
            try:
                # "task_id,window_end,trigger_ts,count_actual,count_expected,loss_network,lag_system,watermark_setting"
                df = pd.read_csv(sf, on_bad_lines='skip')
                if not df.empty and 'lag_system' in df.columns and 'window_end' in df.columns:
                    all_window_data.append(df[['window_end', 'lag_system']].dropna())
            except Exception as e:
                print(f"  Error reading {sf}: {e}")
        
        # 合并所有窗口数据
        if all_window_data:
            combined_df = pd.concat(all_window_data, ignore_index=True)
            
            # 排除首尾 N 秒的窗口数据（基于 window_end 时间戳）
            # window_end 是毫秒时间戳
            min_window_end = combined_df['window_end'].min()
            max_window_end = combined_df['window_end'].max()
            start_cutoff = min_window_end + (exclude_first * 1000)  # 排除开头
            end_cutoff = max_window_end - (exclude_last * 1000)     # 排除结尾

            
            filtered_df = combined_df[
                (combined_df['window_end'] >= start_cutoff) & 
                (combined_df['window_end'] <= end_cutoff)
            ]
            
            if not filtered_df.empty:
                avg_latency = filtered_df['lag_system'].mean()
                p99_latency = filtered_df['lag_system'].quantile(0.99)
            else:
                avg_latency = 0
                p99_latency = 0
        else:
            avg_latency = 0
            p99_latency = 0
        
        
        drop_rate = (drop_count / DEFAULT_TOTAL_LOGS) * 100
        
        results.append({
            'Key': key,
            'Mode': info['mode'],
            'Name': info['original_name'],
            'Lag': info['lag'],
            'DropRate': drop_rate,
            'AvgLatency': avg_latency,
            'P99Latency': p99_latency,
            'WindowSize': info['sz'],
            'SlideStep': info['tp']
        })
        
    return pd.DataFrame(results)


def plot_variability(df):
    """Plot all Dist runs to show variability/network instability."""
    dist_df = df[df['Mode'] == 'Dist']
    if dist_df.empty:
        return

    plt.figure(figsize=(10, 6))
    
    # Group by name/trial to assign different markers/colors if needed
    # For now just scatter all points
    plt.scatter(dist_df['Lag'], dist_df['DropRate'], c='red', alpha=0.6, label='Dist Run')
    
    # Highlight the "Best" curve
    best_dist = dist_df.loc[dist_df.groupby('Lag')['DropRate'].idxmin()].sort_values(by='Lag')
    plt.plot(best_dist['Lag'], best_dist['DropRate'], 'r--', label='Best Dist Trend')

    plt.title('Distributed Environment Variability (Drop Rate)')
    plt.xlabel('Lag (ms)')
    plt.ylabel('Drop Rate (%)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'dist_variability.png'))

def plot_comparisons(df):
    """Plot Single vs Best Dist comparison."""
    single_df = df[df['Mode'] == 'Single'].sort_values(by='Lag')
    
    # Get best Dist runs (lowest drop rate per Lag)
    dist_df = df[df['Mode'] == 'Dist']
    if dist_df.empty:
        print("No Dist data for comparison.")
        return
        
    best_dist = dist_df.loc[dist_df.groupby('Lag')['DropRate'].idxmin()].sort_values(by='Lag')

    # Shared X axis values for interpolation if needed, but here just plot available points
    
    # 1. Drop Rate Comparison
    plt.figure(figsize=(10, 6))
    plt.plot(single_df['Lag'], single_df['DropRate'], 'o-', label='Single (Local)')
    plt.plot(best_dist['Lag'], best_dist['DropRate'], 's--', label='Dist (Campus Network)')
    
    plt.title('Single vs Distributed: Drop Rate Impact')
    plt.xlabel('Lag (ms)')
    plt.ylabel('Drop Rate (%)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'compare_drop_rate.png'))
    
    # 2. Latency Comparison
    plt.figure(figsize=(10, 6))
    plt.plot(single_df['Lag'], single_df['AvgLatency'], 'o-', label='Single Avg Latency')
    plt.plot(best_dist['Lag'], best_dist['AvgLatency'], 's--', label='Dist Avg Latency')
    
    plt.title('Single vs Distributed: Average Latency')
    plt.xlabel('Lag (ms)')
    plt.ylabel('Latency (ms)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'compare_latency.png'))

def plot_overview(df):
    """Overview plot showing Drop Rate and Latency vs Lag for all experiments."""
    df = df.sort_values(by='Lag')
    
    single_df = df[df['Mode'] == 'Single']
    dist_df = df[df['Mode'] == 'Dist']
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # 1. Drop Rate vs Lag
    ax1 = axes[0]
    if not single_df.empty:
        ax1.plot(single_df['Lag'], single_df['DropRate'], 'o-', label='Single', color='blue')
    if not dist_df.empty:
        ax1.scatter(dist_df['Lag'], dist_df['DropRate'], marker='x', c='red', alpha=0.6, label='Dist (All)')
        best_dist = dist_df.loc[dist_df.groupby('Lag')['DropRate'].idxmin()].sort_values(by='Lag')
        ax1.plot(best_dist['Lag'], best_dist['DropRate'], 'r--', label='Dist (Best)')
        
    ax1.set_xlabel('Lag (ms)')
    ax1.set_ylabel('Drop Rate (%)')
    ax1.set_title('丢包率 vs Watermark 延迟')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # 2. Latency vs Lag
    ax2 = axes[1]
    if not single_df.empty:
        ax2.plot(single_df['Lag'], single_df['AvgLatency'], 'o-', label='Single Avg', color='blue')
    if not dist_df.empty:
        best_dist = dist_df.loc[dist_df.groupby('Lag')['DropRate'].idxmin()].sort_values(by='Lag')
        ax2.plot(best_dist['Lag'], best_dist['AvgLatency'], 'x--', label='Dist Avg (Best)', color='red')
        
    ax2.set_xlabel('Lag (ms)')
    ax2.set_ylabel('Latency (ms)')
    ax2.set_title('平均系统延迟 vs Lag (已过滤尾部异常)')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'overview_plot.png'), dpi=150)
    print("Saved overview_plot.png")

def filter_prefer_remake(df):
    """
    对于相同 Mode + Lag 的实验，优先保留 remake 版本。
    如果有 remake2，优先 remake2；否则 remake；否则原版。
    """
    def get_priority(name):
        name_lower = name.lower()
        if 'remake2' in name_lower:
            return 2
        elif 'remake' in name_lower:
            return 1
        else:
            return 0
    
    df['_priority'] = df['Name'].apply(get_priority)
    
    # 按 Mode + Lag 分组，取优先级最高的
    idx = df.groupby(['Mode', 'Lag'])['_priority'].idxmax()
    filtered_df = df.loc[idx].drop(columns=['_priority'])
    
    return filtered_df.reset_index(drop=True)

def main():
    parser = argparse.ArgumentParser(description='Flink Watermark 实验数据分析')
    parser.add_argument('--exclude-first', type=int, default=DEFAULT_EXCLUDE_FIRST,
                        help=f'排除开头 N 秒的数据（默认: {DEFAULT_EXCLUDE_FIRST}）')
    parser.add_argument('--exclude-last', type=int, default=DEFAULT_EXCLUDE_LAST,
                        help=f'排除结尾 N 秒的数据（默认: {DEFAULT_EXCLUDE_LAST}）')
    parser.add_argument('--no-prefer-remake', action='store_true',
                        help='不优先使用 remake 版本（默认: 优先 remake）')
    parser.add_argument('--all-data', action='store_true',
                        help='显示所有实验数据（不过滤 remake）')
    args = parser.parse_args()
    
    # 设置全局参数
    global EXCLUDE_FIRST_SECONDS, EXCLUDE_LAST_SECONDS
    EXCLUDE_FIRST_SECONDS = args.exclude_first
    EXCLUDE_LAST_SECONDS = args.exclude_last
    
    print(f"参数: 排除开头 {EXCLUDE_FIRST_SECONDS}s, 排除结尾 {EXCLUDE_LAST_SECONDS}s")
    
    experiments = load_data()
    df = analyze_experiments(experiments, EXCLUDE_FIRST_SECONDS, EXCLUDE_LAST_SECONDS)
    
    # Clean data (remove unknown lag)
    df = df[df['Lag'] >= 0]
    
    # 优先使用 remake 版本
    if not args.no_prefer_remake and not args.all_data:
        df_filtered = filter_prefer_remake(df)
        print(f"\n已过滤 remake 版本，保留 {len(df_filtered)} 条实验（原 {len(df)} 条）")
        df = df_filtered
    
    print("\nAnalysis Results:")
    print(df[['Mode', 'Name', 'Lag', 'DropRate', 'AvgLatency', 'P99Latency']].to_string())
    
    df.to_csv(os.path.join(PLOTS_DIR, 'summary_metrics.csv'), index=False)
    
    plot_overview(df)
    plot_variability(df)
    plot_comparisons(df)
    print(f"\nAll plots saved to {PLOTS_DIR}")

if __name__ == "__main__":
    main()
