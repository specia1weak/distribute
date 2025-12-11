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
    """
    match = re.search(r"(\d+(\.\d+)?)klag", dir_name)
    if match:
        return float(match.group(1))
    return None

def analyze_aggregation():
    print(f"🚀 开始聚合分析...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    summary_files = glob.glob(os.path.join(ANALYSIS_DIR, "*", "summary.txt"))
    
    data = []
    
    for summary_file in summary_files:
        dir_name = os.path.basename(os.path.dirname(summary_file))
        lag = parse_lag_from_name(dir_name)
        
        if lag is None:
            continue
            
        # 读取基本 summary.txt
        drop_rate = 0.0
        avg_latency = 0.0
        with open(summary_file, 'r', encoding='utf-8') as f:
            content = f.read()
            match_drop = re.search(r"总丢弃率:\s*([\d\.]+)%", content)
            if match_drop:
                drop_rate = float(match_drop.group(1)) / 100.0
            
            match_latency = re.search(r"平均处理延迟:\s*([\d\.]+)\s*s", content)
            if match_latency:
                avg_latency = float(match_latency.group(1))

        # 读取 Window Trigger Delay (如果存在)
        # 查找同目录下的 summary_window.txt
        # 注意: analyze_single_experiment 是按 prefix 生成目录的
        # 但是 summary.txt 是在 'prefix' 目录下
        # 我们的 analyze_window_experiment 也会生成在 'prefix' 目录下吗？
        # 是的，output_base_dir/prefix/summary_window.txt
        # 但是，experiment_prefix 对 trace 和 window 文件是不一样的！
        # Trace: 3p0.5klag-all-data-trace_P3
        # Window: 3p0.5klag-window_Tumbling_P3
        # 它们会生成在两个不同的文件夹里！
        # 我们需要在聚合时把它们关联起来。
        # 关联键: Lag + Type
        
        # 现在的 summary_file 是 Trace 的 (based on "all-data-trace" usually)
        # 如果是 Window 的 summary (如果 Window 也会生成 summary.txt? No, window gen summary_window.txt)
        
        # Wait, analyze_single_experiment main flow calculates summary.txt.
        # It handles "all-data-trace".
        # Window flow generates "summary_window.txt".
        # Window files have different prefix.
        # So we have separate Data entries.
        # Let's collect ALL and then merge.
        
        experiment_type = "Unknown"
        if "slide21k" in dir_name:
            experiment_type = "Sliding Window"
        elif "3p" in dir_name and "trace" in dir_name:
            experiment_type = "Tumbling Window (Trace)"
        elif "3p" in dir_name and "window" in dir_name: # Likely won't match here because we glob 'summary.txt'?
             pass 

        # Current loop logic is based on summary.txt existence.
        # We need to explicitly look for window summaries too.
        
        data.append({
            "Experiment": dir_name,
            "Lag": lag,
            "DropRate": drop_rate,
            "AvgLatency": avg_latency,
            "TriggerDelay": None, # Will fill later
            "Type": experiment_type
        })

    # Search for summary_window.txt
    window_files = glob.glob(os.path.join(ANALYSIS_DIR, "*", "summary_window.txt"))
    window_data = []
    for wf in window_files:
        dir_name = os.path.basename(os.path.dirname(wf))
        lag = parse_lag_from_name(dir_name)
        
        trigger_delay = 0.0
        with open(wf, 'r', encoding='utf-8') as f:
             content = f.read()
             match = re.search(r"平均窗口触发延迟:\s*([\d\.]+)\s*s", content)
             if match:
                 trigger_delay = float(match.group(1))
        
        window_data.append({
            "Lag": lag,
            "TriggerDelay": trigger_delay,
            "Type": "Tumbling Window" # Assuming 3p* are Tumbling
        })
        
    df = pd.DataFrame(data)
    df_window = pd.DataFrame(window_data)
    
    # Merge Trigger Delay into Main DF
    # 我们只关心 Tumbling Window 的合并
    # Trace Type: "Tumbling Window (Trace)"
    # Window Type: "Tumbling Window"
    
    # 简单的合并逻辑：按 Lag merge
    # 因为 Lag 是唯一的 key for 3p experiments
    
    final_data = []
    
    # Filter for Tumbling Trace
    tumbling_df = df[df['Type'] == "Tumbling Window (Trace)"]
    
    for _, row in tumbling_df.iterrows():
        lag = row['Lag']
        # Find matching window data
        match = df_window[df_window['Lag'] == lag]
        trigger = match.iloc[0]['TriggerDelay'] if not match.empty else 0.0
        
        row['TriggerDelay'] = trigger
        row['Type'] = "Tumbling Window" # Rename back
        final_data.append(row)
        
    # Also add Sliding Window (no trigger delay data usually unless we analyze it)
    sliding_df = df[df['Type'] == "Sliding Window"]
    for _, row in sliding_df.iterrows():
        row['TriggerDelay'] = 0.0 # Not calculated
        final_data.append(row)
        
    df_final = pd.DataFrame(final_data)
    df_final = df_final.sort_values("Lag")
    
    print("📊 聚合数据预览:")
    print(df_final[['Lag', 'DropRate', 'AvgLatency', 'TriggerDelay', 'Type']])
    
    df_final.to_csv(os.path.join(OUTPUT_DIR, "aggregation_metrics.csv"), index=False)
    
    # --- 图表 1: Drop Rate vs Lag (Restored & Fixed) ---
    plt.figure(figsize=(10, 6))
    types = df_final['Type'].unique()
    markers = ['o', 's', '^', 'D']
    
    for i, exp_type in enumerate(types):
        subset = df_final[df_final['Type'] == exp_type]
        plt.plot(subset['Lag'], subset['DropRate'] * 100, 
                 marker=markers[i % len(markers)], linestyle='-', linewidth=2, markersize=8, 
                 label=exp_type)
        
        for _, row in subset.iterrows():
            # Fix: Use annotate with offset points to avoid scale-dependent label offsets
            label_text = f"{row['DropRate']*100:.1f}%" if row['DropRate'] > 0 else "0%"
            # 只在非0或者特定的点标示，防止拥挤？或者全部标示。
            # 这里全部标示，但使用 offset
            plt.annotate(label_text, 
                         xy=(row['Lag'], row['DropRate'] * 100),
                         xytext=(0, 5),  # 5 points vertical offset
                         textcoords='offset points',
                         ha='center', va='bottom', fontsize=9)

    plt.title("Drop Rate vs Watermark Lag", fontsize=14)
    plt.xlabel("Watermark Lag (s)", fontsize=12)
    plt.ylabel("Drop Rate (%)", fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    save_path = os.path.join(OUTPUT_DIR, "drop_rate_vs_lag.png")
    plt.savefig(save_path)
    print(f"🖼️ [1/2] 丢弃率图表已保存: {save_path}")

    # Plotting Trade-off with Trigger Delay
    target_type = "Tumbling Window"
    subset = df_final[df_final['Type'] == target_type]
    
    if not subset.empty:
        fig, ax1 = plt.subplots(figsize=(10, 6))
        
        color = 'tab:red'
        ax1.set_xlabel('Watermark Lag (s)', fontsize=12)
        ax1.set_ylabel('Drop Rate (%)', color=color, fontsize=12)
        l1, = ax1.plot(subset['Lag'], subset['DropRate'] * 100, color=color, marker='o', label='Drop Rate')
        ax1.tick_params(axis='y', labelcolor=color)
        ax1.grid(True, linestyle='--', alpha=0.5)

        ax2 = ax1.twinx()  
        color = 'tab:blue'
        ax2.set_ylabel('Time (s)', color=color, fontsize=12) # Shared label for Latency and Trigger
        
        # Line 2: Processing Latency
        l2, = ax2.plot(subset['Lag'], subset['AvgLatency'], color=color, marker='s', linestyle='--', label='Processing Latency')
        
        # Line 3: Trigger Latency (New)
        color_trigger = 'tab:purple'
        l3, = ax2.plot(subset['Lag'], subset['TriggerDelay'], color=color_trigger, marker='^', linestyle='-.', label='Window Trigger Latency')
        
        ax2.tick_params(axis='y', labelcolor=color) # Keep blue for clarity or mix?
        ax2.grid(False) 
        ax2.set_ylim(bottom=0)

        plt.title(f"Trade-off Analysis: Accuracy vs Latency ({target_type})", fontsize=14)
        
        lines = [l1, l2, l3]
        labels = [l.get_label() for l in lines]
        ax1.legend(lines, labels, loc='upper center')
        
        save_path_2 = os.path.join(OUTPUT_DIR, "tradeoff_analysis.png")
        plt.savefig(save_path_2)
        print(f"🖼️ [2/2] 权衡分析图表已保存: {save_path_2}")

if __name__ == "__main__":
    analyze_aggregation()
