import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import argparse
import numpy as np

# ================= 配置区 =================
SAMPLE_RATE = 0.2  # 必须与 Flink 代码一致


# ==========================================

def analyze_experiment(exp_dir):
    print(f"🚀 [增强版] 分析实验数据: {exp_dir}")

    # 1. 读取数据
    path_win = os.path.join(exp_dir, "window_stats.csv")
    path_late = os.path.join(exp_dir, "late_data.csv")
    path_accept = os.path.join(exp_dir, "accepted_latency.csv")

    try:
        df_win = pd.read_csv(path_win)
        wm_setting = df_win['watermark_setting'].iloc[0] if not df_win.empty else 0
    except:
        print("❌ 错误：找不到 window_stats.csv")
        return

    # 读取 Late Data (处理空文件情况)
    try:
        df_late = pd.read_csv(path_late)
    except:
        df_late = pd.DataFrame(columns=['system_ts', 'lag_magnitude'])

    # 读取 Accepted Data
    try:
        df_accept = pd.read_csv(path_accept)
    except:
        df_accept = pd.DataFrame(columns=['latency'])

    # 2. 核心：统一时间基准 (Global Time Offset)
    # 以第一个窗口结束时间作为 "实验第0秒"
    start_time = df_win['window_end'].min()
    print(f"⏱️ 时间基准 (T0): {start_time}")

    # 转换相对时间 (秒)
    df_win['rel_time'] = (df_win['window_end'] - start_time) / 1000.0

    if not df_late.empty:
        # 注意：这里必须用同一个 start_time 减，才能和上面的图对齐！
        df_late['rel_time'] = (df_late['system_ts'] - start_time) / 1000.0

    # 计算实验总时长 (用于设置 X 轴范围)
    max_duration = df_win['rel_time'].max()

    # 3. 统计指标计算
    count_late = len(df_late)
    count_accept_est = int(len(df_accept) / SAMPLE_RATE)
    total_est = count_accept_est + count_late
    drop_rate = (count_late / total_est * 100) if total_est > 0 else 0

    print("-" * 40)
    print(f"📊 实验报告: {os.path.basename(exp_dir)}")
    print(f"⚙️  Watermark Lag: {wm_setting} ms")
    print(f"📉 最终丢弃率: {drop_rate:.2f}% (Late: {count_late}, Total: {total_est})")
    print("-" * 40)

    # 4. 可视化绘图
    sns.set_theme(style="whitegrid")
    fig = plt.figure(figsize=(16, 12))
    gs = fig.add_gridspec(2, 2)

    # --- 图1: 延迟分布 (直方图) ---
    ax1 = fig.add_subplot(gs[0, :])

    # 智能过滤异常值 (只展示 99.5% 的数据，防止极值拉伸坐标轴)
    valid_accept = df_accept['latency']
    p99_latency = valid_accept.quantile(0.995) if not valid_accept.empty else 10000
    ax1.set_xlim(0, max(p99_latency, wm_setting * 2))  # 动态设置 X 轴范围

    sns.histplot(data=df_accept, x='latency', color='tab:green', stat='density', alpha=0.5, label='Accepted (Sampled)',
                 ax=ax1, binwidth=100)
    if not df_late.empty:
        sns.histplot(data=df_late, x='lag_magnitude', color='tab:red', stat='density', alpha=0.6,
                     label='Dropped (Late)', ax=ax1, binwidth=100)

    ax1.axvline(x=wm_setting, color='black', linestyle='--', linewidth=2, label=f'Watermark ({wm_setting}ms)')
    ax1.set_title(f"Latency Distribution (Lag={wm_setting}ms)", fontsize=14)
    ax1.legend()

    # --- 图2: 吞吐量 (柱状图) ---
    ax2 = fig.add_subplot(gs[1, 0])
    sns.barplot(data=df_win, x='rel_time', y='count_actual', ax=ax2, color='tab:blue', alpha=0.4)

    # 修正 Barplot X轴标签过密的问题
    ax2.set_xticks(np.arange(0, len(df_win), 5))  # 每5个窗口显示一个标签
    ax2.set_title("Throughput over Time")
    ax2.set_ylabel("Count")

    # --- 图3: 丢包时间线 (散点图) ---
    ax3 = fig.add_subplot(gs[1, 1])

    if not df_late.empty:
        # 强制设置 X 轴范围与实验时长一致，防止“挤在左边”
        ax3.set_xlim(0, max_duration + 5)

        sns.scatterplot(data=df_late, x='rel_time', y='lag_magnitude', color='red', alpha=0.6, s=15, ax=ax3)
        ax3.set_title(f"Dropped Data Timeline (Total: {count_late})")
        ax3.set_xlabel("Experiment Time (seconds)")
        ax3.set_ylabel("Lateness (ms)")
    else:
        ax3.text(0.5, 0.5, "No Data Dropped 🎉", ha='center', va='center', fontsize=14)
        ax3.set_xlim(0, max_duration + 5)

    plt.tight_layout()
    output_path = os.path.join(exp_dir, "analysis_report_v2.png")
    plt.savefig(output_path)
    print(f"✅ 图表已保存: {output_path}")


if __name__ == '__main__':

    analyze_experiment("exp_tumbling_lag0")