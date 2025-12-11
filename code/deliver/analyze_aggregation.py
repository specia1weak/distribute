import os
import glob
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

ANALYSIS_DIR = r"d:\pyprojects\distribute\data\analysis"
OUTPUT_DIR = r"d:\pyprojects\distribute\data\conclusion"

sns.set_style("whitegrid")
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

def parse_lag_from_name(dir_name):
    """
    从目录名解析 lag 值。
    例如: 
    '3p0.5klag-all-data-trace_P3' -> 0.5
    'slide21k-1.5klag_Sliding_SZ2000_SL1000_P3' -> 1.5
    '3p2klag-...' -> 2.0
    """
    # 匹配 数字+klag
    match = re.search(r"(\d+(\.\d+)?)klag", dir_name)
    if match:
        return float(match.group(1))
    return None

def analyze_aggregation():
    print(f"🚀 开始聚合分析...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 寻找所有的 summary.txt
    summary_files = glob.glob(os.path.join(ANALYSIS_DIR, "*", "summary.txt"))
    
    data = []
    
    for summary_file in summary_files:
        dir_name = os.path.basename(os.path.dirname(summary_file))
        lag = parse_lag_from_name(dir_name)
        
        if lag is None:
            print(f"⚠️ 无法从 {dir_name} 解析 Lag，跳过。")
            continue
            
        # 读取 metrics csv 获取最终的累积丢弃率
        # (虽然 summary.txt 也有，但 metrics.csv 更方便读取最后一行)
        # 或者从 summary.txt 读
        # 用 summary.txt 比较稳，因为是最后生成的
        drop_rate = 0.0
        with open(summary_file, 'r', encoding='utf-8') as f:
            content = f.read()
            # 查找 "总丢弃率: 4.00%"
            # 查找 "总丢弃率: 4.00%"
            match_drop = re.search(r"总丢弃率:\s*([\d\.]+)%", content)
            if match_drop:
                drop_rate = float(match_drop.group(1)) / 100.0
            else:
                print(f"⚠️ 从 {summary_file} 无法解析丢弃率")
                drop_rate = 0.0
            
            # 查找 "平均处理延迟: 0.1234 s"
            match_latency = re.search(r"平均处理延迟:\s*([\d\.]+)\s*s", content)
            if match_latency:
                avg_latency = float(match_latency.group(1))
            else:
                print(f"⚠️ 从 {summary_file} 无法解析延迟")
                avg_latency = 0.0

        # 区分实验类型
        experiment_type = "Unknown"
        if "slide21k" in dir_name:
            experiment_type = "Sliding Window"
        elif "3p" in dir_name or "Tumbling" in dir_name:
            experiment_type = "Tumbling Window"
        else:
            experiment_type = "Other"
            
        data.append({
            "Experiment": dir_name,
            "Lag": lag,
            "DropRate": drop_rate,
            "AvgLatency": avg_latency,
            "Type": experiment_type
        })
    
    if not data:
        print("❌ 没有收集到数据！")
        return
        
    df = pd.DataFrame(data)
    df = df.sort_values("Lag")
    
    print("📊 聚合数据预览:")
    print(df[['Experiment', 'Lag', 'DropRate', 'AvgLatency', 'Type']])
    
    # 保存聚合数据
    df.to_csv(os.path.join(OUTPUT_DIR, "aggregation_metrics.csv"), index=False)
    
    # --- 图表 1: Drop Rate vs Lag ---
    plt.figure(figsize=(10, 6))
    types = df['Type'].unique()
    markers = ['o', 's', '^', 'D']
    
    for i, exp_type in enumerate(types):
        subset = df[df['Type'] == exp_type]
        plt.plot(subset['Lag'], subset['DropRate'] * 100, 
                 marker=markers[i % len(markers)], linestyle='-', linewidth=2, markersize=8, 
                 label=exp_type)
        
        for _, row in subset.iterrows():
            plt.text(row['Lag'], row['DropRate'] * 100 + 0.5, f"{row['DropRate']*100:.1f}%", ha='center')

    plt.title("Drop Rate vs Watermark Lag", fontsize=14)
    plt.xlabel("Watermark Lag (s)", fontsize=12)
    plt.ylabel("Drop Rate (%)", fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    save_path = os.path.join(OUTPUT_DIR, "drop_rate_vs_lag.png")
    plt.savefig(save_path)
    print(f"🖼️ [1/2] 丢弃率图表已保存: {save_path}")
    
    # --- 图表 2: Trade-off Analysis (Dual Axis) ---
    # 只绘制 Tumbling Window (作为主要分析对象) 或者都画
    # 为了清晰，我们针对每种类型画一张，或者只画 Tumbling
    target_type = "Tumbling Window"
    subset = df[df['Type'] == target_type]
    
    if not subset.empty:
        fig, ax1 = plt.subplots(figsize=(10, 6))
        
        # 左轴: 丢弃率 (Drop Rate)
        color = 'tab:red'
        ax1.set_xlabel('Watermark Lag (s)', fontsize=12)
        ax1.set_ylabel('Drop Rate (%)', color=color, fontsize=12)
        l1, = ax1.plot(subset['Lag'], subset['DropRate'] * 100, color=color, marker='o', label='Drop Rate')
        ax1.tick_params(axis='y', labelcolor=color)
        ax1.grid(True, linestyle='--', alpha=0.5)

        # 右轴: 延迟 (Latency)
        ax2 = ax1.twinx()  
        color = 'tab:blue'
        ax2.set_ylabel('Average Latency (s)', color=color, fontsize=12)
        l2, = ax2.plot(subset['Lag'], subset['AvgLatency'], color=color, marker='s', linestyle='--', label='Latency')
        ax2.tick_params(axis='y', labelcolor=color)
        ax2.grid(False) # 右轴不画网格，避免混乱

        plt.title(f"Trade-off Analysis: Accuracy vs Latency ({target_type})", fontsize=14)
        
        # 合并图例
        lines = [l1, l2]
        labels = [l.get_label() for l in lines]
        ax1.legend(lines, labels, loc='upper center')
        
        save_path_2 = os.path.join(OUTPUT_DIR, "tradeoff_analysis.png")
        plt.savefig(save_path_2)
        print(f"🖼️ [2/2] 权衡分析图表已保存: {save_path_2}")
    else:
        print(f"⚠️ 没有找到 {target_type} 的数据，跳过 Trade-off 图表")

if __name__ == "__main__":
    analyze_aggregation()
