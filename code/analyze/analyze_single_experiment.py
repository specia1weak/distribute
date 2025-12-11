import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import glob
import os
import argparse

# 设置绘图风格
sns.set_style("whitegrid")
# 设置支持中文（如果系统支持）
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS'] 
plt.rcParams['axes.unicode_minus'] = False

def analyze_experiment(data_dir, experiment_prefix, output_base_dir, cutoff_seconds=10):
    """
    分析单个实验的所有任务数据。
    
    参数:
        data_dir: 数据源目录 (例如 'data/new')
        experiment_prefix: 实验前缀 (例如 '3p0.5klag-all-data-trace_P3')
        output_base_dir: 输出结果的根目录
        cutoff_seconds: 剔除最后多少秒的数据 (默认 10)
    """
    print(f"Start analyzing: {experiment_prefix} (cutoff last {cutoff_seconds}s)")
    
    # 1. 查找匹配的文件
    # 模式匹配: prefix_task-*.csv
    # 注意：文件名可能是 '3p0.5klag-all-data-trace_P3_task-0.csv'
    search_pattern = os.path.join(data_dir, f"{experiment_prefix}_task-*.csv")
    files = glob.glob(search_pattern)
    
    if not files:
        print(f"No match files found: {search_pattern}")
        return

    print(f"Found {len(files)} files: {[os.path.basename(f) for f in files]}")

    # 2. 读取并合并数据
    dfs = []
    for f in files:
        try:
            # 假设 CSV 有 header
            df_temp = pd.read_csv(f)
            dfs.append(df_temp)
        except Exception as e:
            print(f"Failed to read file {f}: {e}")

    if not dfs:
        return

    df = pd.concat(dfs, ignore_index=True)
    
    # 3. 数据预处理
    
    # 统一时间列名
    # Trace file: sys_ts, event_ts
    # Window file: trigger_ts, window_end
    # 我们主要分析 Trace file 算指标，但是 Trigger Delay 需要 Window file
    # 这里我们只处理 Trace file 的逻辑，如果传入的是 Window file，可能需要另一套逻辑
    # 但 analyze_single_experiment 之前是针对 Trace file 设计的 (metrics_1s based on time_sec)
    
    if 'trigger_ts' in df.columns and 'window_end' in df.columns:
         analyze_window_experiment(df, experiment_prefix, output_base_dir, cutoff_seconds)
         return

    # 转换时间戳 (假设是毫秒)
    df['sys_ts'] = df['sys_ts'].astype(float)
    df['event_ts'] = df['event_ts'].astype(float)
    
    # 排序
    df = df.sort_values('sys_ts').reset_index(drop=True)
    
    # 剔除最后的 cutoff_seconds 数据
    # 原因：实验结束时可能没有新的日志推进水位线，导致最后的数据无法触发窗口
    max_sys_ts = df['sys_ts'].max()
    cutoff_ts = max_sys_ts - (cutoff_seconds * 1000) # seconds to ms
    
    initial_count = len(df)
    df = df[df['sys_ts'] <= cutoff_ts].copy()
    filtered_count = len(df)
    print(f"Data cleaning: cutoff last {cutoff_seconds}s. Count {initial_count} -> {filtered_count} (Removed {initial_count - filtered_count})")
    
    if df.empty:
        print("Data is empty after cleaning!")
        return

    # 4. 指标计算
    # 延迟 (秒) = 处理时间 - 事件时间
    df['delay_s'] = (df['sys_ts'] - df['event_ts']) / 1000.0
    
    # 状态判定
    # 假设 'LATE' 是丢弃，其他都是正常处理
    df['is_late'] = df['status'] == 'LATE'
    df['is_processed'] = ~df['is_late']
    
    # 时间对齐用于绘图 (相对实验开始时间)
    start_sys_ts = df['sys_ts'].min()
    df['relative_time_s'] = (df['sys_ts'] - start_sys_ts) / 1000.0
    
    # 转换为整数秒用于 GroupBy (避免 Timedelta 对齐问题)
    df['time_sec'] = df['relative_time_s'].astype(int)
    
    # 先做 1s 粒度的基础聚合
    metrics_1s = df.groupby('time_sec').size().to_frame(name='total_count')
    metrics_1s['processed_count'] = df[df['is_processed']].groupby('time_sec').size()
    metrics_1s['late_count'] = df[df['is_late']].groupby('time_sec').size()
    metrics_1s['processed_delay_sum'] = df[df['is_processed']].groupby('time_sec')['delay_s'].sum()
    
    # 补全时间轴 (Reindex) 确保连续性
    if not metrics_1s.empty:
        max_sec = metrics_1s.index.max()
        all_seconds = range(0, max_sec + 1)
        metrics_1s = metrics_1s.reindex(all_seconds, fill_value=0)
    
    metrics_1s = metrics_1s.fillna(0)
    
    # 5s 滚动窗口平滑 (Center=True 表示统计"附近"的数据)
    window_size = 5
    print(f"Applying {window_size}s rolling window smoothing...")
    
    rolled_sum = metrics_1s.rolling(window=window_size, min_periods=1, center=True).sum()
    rolled_mean = metrics_1s.rolling(window=window_size, min_periods=1, center=True).mean()
    
    metrics = pd.DataFrame(index=metrics_1s.index)
    metrics['relative_time_s'] = metrics.index # 索引已经是秒(int)
    
    # 吞吐量用 mean (即 5s 内的平均每秒吞吐)
    metrics['throughput'] = rolled_mean['total_count']
    metrics['processed_count'] = rolled_mean['processed_count']
    
    # 比率类指标用 sum 之比 (加权平均)
    # 丢弃率使用累积值 (Cumulative)
    # 时刻 x 的丢弃率 = 从开始到 x 的所有 LATE / 所有 Total
    cumsum_late = metrics_1s['late_count'].cumsum()
    cumsum_total = metrics_1s['total_count'].cumsum()
    metrics['drop_rate'] = cumsum_late / cumsum_total.replace(0, np.nan)
    
    # 平均延迟保持滚动平均 (体现局部趋势)
    metrics['avg_delay_processed'] = rolled_sum['processed_delay_sum'] / rolled_sum['processed_count'].replace(0, np.nan)
    
    # 填充 NaN
    metrics = metrics.fillna(0)
    
    # 5. 输出结果
    # 创建输出目录
    # 实验名前缀作为目录名
    output_dir = os.path.join(output_base_dir, experiment_prefix)
    os.makedirs(output_dir, exist_ok=True)
    
    # 保存 CSV
    csv_path = os.path.join(output_dir, "analysis_metrics.csv")
    metrics.to_csv(csv_path, index=False)
    print(f"Metrics saved: {csv_path}")

    # 6. 可视化
    plot_path = os.path.join(output_dir, "analysis_plot.png")
    plot_analysis(metrics, experiment_prefix, plot_path)
    print(f"Plot saved: {plot_path}")
    
    # 7. 全局统计总结
    total_processed = df['is_processed'].sum()
    total_late = df['is_late'].sum()
    total_drop_rate = total_late / len(df) if len(df) > 0 else 0
    avg_latency = df[df['is_processed']]['delay_s'].mean()
    
    summary_path = os.path.join(output_dir, "summary.txt")
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write(f"实验: {experiment_prefix}\n")
        f.write(f"总数据量(清洗后): {len(df)}\n")
        f.write(f"处理数据量: {total_processed}\n")
        f.write(f"丢弃数据量(LATE): {total_late}\n")
        f.write(f"总丢弃率: {total_drop_rate:.2%}\n")
        f.write(f"平均处理延迟: {avg_latency:.4f} s\n")
    print(f"Summary saved: {summary_path}")


def analyze_window_experiment(df, experiment_prefix, output_base_dir, cutoff_seconds=10):
    """
    专门分析 Window Metrics 文件 (包含 trigger_ts, window_end)
    计算 Window Trigger Latency
    输出 summary_window.txt
    """
    print(f"Analyzing Window Metrics: {experiment_prefix}")
    
    # 转换
    df['trigger_ts'] = df['trigger_ts'].astype(float)
    df['window_end'] = df['window_end'].astype(float)
    
    # Cutoff
    max_ts = df['trigger_ts'].max()
    cutoff_ts = max_ts - (cutoff_seconds * 1000)
    
    df = df[df['trigger_ts'] <= cutoff_ts].copy()
    
    if df.empty: return

    # Calculate Trigger Delay = trigger_ts - window_end
    # Note: trigger_ts is system time, window_end is event time.
    # Unit: ms -> s
    df['trigger_delay_s'] = (df['trigger_ts'] - df['window_end']) / 1000.0
    
    avg_trigger_delay = df['trigger_delay_s'].mean()
    
    # Save partial summary
    output_dir = os.path.join(output_base_dir, experiment_prefix)
    os.makedirs(output_dir, exist_ok=True)
    summary_path = os.path.join(output_dir, "summary_window.txt")
    
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write(f"平均窗口触发延迟: {avg_trigger_delay:.4f} s\n")
        
    print(f"Window Summary saved: {summary_path}")


def plot_analysis(metrics, title, save_path):
    fig, axes = plt.subplots(3, 1, figsize=(12, 12), sharex=True)
    
    # 1. 吞吐量
    ax1 = axes[0]
    ax1.plot(metrics['relative_time_s'], metrics['throughput'], label='Total Input', color='tab:blue')
    ax1.plot(metrics['relative_time_s'], metrics['processed_count'], label='Processed', color='tab:green', linestyle='--')
    ax1.set_ylabel("吞吐量 (events/s)")
    ax1.set_title(f"{title} - 吞吐量趋势")
    ax1.legend()
    ax1.grid(True)
    
    # 2. 延迟
    ax2 = axes[1]
    ax2.plot(metrics['relative_time_s'], metrics['avg_delay_processed'], color='tab:orange', label='Avg Delay (Processed)')
    ax2.set_ylabel("平均延迟 (s)")
    ax2.set_title("处理延迟趋势")
    ax2.legend()
    ax2.grid(True)
    
    # 3. 丢弃率
    ax3 = axes[2]
    ax3.plot(metrics['relative_time_s'], metrics['drop_rate'], color='tab:red', label='Cumulative Drop Rate')
    ax3.set_ylabel("累积丢弃率")
    ax3.set_xlabel("实验时间 (s)")
    ax3.set_title("累积丢弃率趋势 (Cumulative)")
    ax3.set_ylim(-0.05, 1.05) # 稍微调整范围
    ax3.legend()
    ax3.grid(True)
    
    plt.tight_layout()
    plt.savefig(save_path)
    plt.close()

if __name__ == "__main__":
    pass
