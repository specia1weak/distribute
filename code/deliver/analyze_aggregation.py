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
            match = re.search(r"总丢弃率:\s*([\d\.]+)%", content)
            if match:
                drop_rate = float(match.group(1)) / 100.0
            else:
                print(f"⚠️ 从 {summary_file} 无法解析丢弃率")
                continue
                
        # 区分实验类型
        experiment_type = "Unknown"
        if "slide21k" in dir_name:
            experiment_type = "SlidingWindow (2k/1k)"
        elif dir_name.startswith("3p"):
            experiment_type = "TumblingWindow (?)"
        else:
            experiment_type = "Other"
            
        data.append({
            "Experiment": dir_name,
            "Lag": lag,
            "DropRate": drop_rate,
            "Type": experiment_type
        })
    
    if not data:
        print("❌ 没有收集到数据！")
        return
        
    df = pd.DataFrame(data)
    df = df.sort_values("Lag")
    
    print("📊 聚合数据预览:")
    print(df)
    
    # 保存聚合数据
    df.to_csv(os.path.join(OUTPUT_DIR, "aggregation_metrics.csv"), index=False)
    
    # 绘图
    plt.figure(figsize=(10, 6))
    
    # 按类型分组绘图
    types = df['Type'].unique()
    markers = ['o', 's', '^', 'D']
    
    for i, exp_type in enumerate(types):
        subset = df[df['Type'] == exp_type]
        plt.plot(subset['Lag'], subset['DropRate'] * 100, 
                 marker=markers[i % len(markers)], linestyle='-', linewidth=2, markersize=8, 
                 label=exp_type)
        
        # 标注点
        for _, row in subset.iterrows():
            plt.text(row['Lag'], row['DropRate'] * 100 + 0.5, f"{row['DropRate']*100:.1f}%", ha='center')

    plt.title("Drop Rate vs Watermark Lag", fontsize=14)
    plt.xlabel("Watermark Lag (s)", fontsize=12)
    plt.ylabel("Drop Rate (%)", fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    save_path = os.path.join(OUTPUT_DIR, "drop_rate_vs_lag.png")
    plt.savefig(save_path)
    print(f"🖼️ 聚合图表已保存: {save_path}")

if __name__ == "__main__":
    analyze_aggregation()
