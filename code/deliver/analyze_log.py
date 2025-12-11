import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# ================= 配置 =================
FILE_PATH = "experiment_data_pro.csv"


# =======================================

def analyze_data():
    print(f"📖 正在读取 {FILE_PATH} ...")
    try:
        df = pd.read_csv(FILE_PATH)
    except FileNotFoundError:
        print("❌ 找不到文件，请先运行生成器生成 CSV。")
        return

    # 1. 基础数据清洗
    # 确保是浮点数
    df['send_offset'] = df['send_offset'].astype(float)
    df['event_offset'] = df['event_offset'].astype(float)

    # 按照发送时间排序（模拟流式读取的顺序）
    df = df.sort_values('send_offset').reset_index(drop=True)

    print("-" * 30)
    print(f"📊 基础统计:")
    print(f"   数据总量: {len(df)}")
    print(f"   持续时长: {df['send_offset'].max():.2f} 秒")
    print(f"   平均延迟: {df['delay'].mean():.4f} 秒")
    print(f"   最大延迟: {df['delay'].max():.4f} 秒")

    # ==========================================
    # 2. 深度指标计算
    # ==========================================

    # A. 流量密度 (Traffic Density)
    # 将 send_offset 转换为 timedelta 以便重采样，这里模拟成时间索引
    df['time_idx'] = pd.to_timedelta(df['send_offset'], unit='s')
    # 每一秒的吞吐量
    throughput = df.set_index('time_idx').resample('1S')['id'].count()

    # B. 乱序/逆序分析 (Out-of-Orderness)
    # 核心逻辑：对于流中的第 i 个元素，它“以为”的当前最大时间是多少？
    # 如果 event_time < max_event_time_seen_so_far，说明它迟到了
    df['max_seen_event'] = df['event_offset'].cummax()
    df['lag'] = df['max_seen_event'] - df['event_offset']
    # 修正浮点数误差，小于0的算0
    df['lag'] = df['lag'].apply(lambda x: x if x > 0 else 0)

    out_of_order_count = (df['lag'] > 0).sum()
    print(f"   逆序数据量: {out_of_order_count} (占比 {out_of_order_count / len(df) * 100:.2f}%)")
    print(f"   最大乱序滞后(Max Lag): {df['lag'].max():.4f} 秒")

    # C. 水位线权衡分析 (Watermark Trade-off)
    # 模拟：如果我设置水位线延迟为 T，会丢多少包？
    # 丢包条件：实际延迟 (delay) > 允许的延迟 (watermark_lag) ???
    # 注意：在理想单流中，如果 lag > watermark_lag，通常会被丢弃/侧输出
    # 这里的 lag 是相对于“当前见过的最大时间”，这正是 Flink Watermark 的生成逻辑

    thresholds = np.linspace(0, 5, 50)  # 测试 0s 到 5s 的水位线设置
    drop_rates = []

    for t in thresholds:
        # 如果某条数据的滞后程度(lag) 超过了设定的阈值(t)，它就会被判定为迟到
        drop_count = (df['lag'] > t).sum()
        drop_rates.append(drop_count / len(df) * 100)

    # ==========================================
    # 3. 可视化绘图
    # ==========================================
    sns.set_style("whitegrid")
    # 设置支持中文（根据系统可能需要调整，这里用英文通用标签避免乱码，或者你可以配置字体）
    plt.rcParams['axes.unicode_minus'] = False

    fig = plt.figure(figsize=(16, 10))
    gs = fig.add_gridspec(2, 2)

    # 图1: 流量密度 (验证正弦波/突发)
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.plot(throughput.index.total_seconds(), throughput.values, color='tab:blue', marker='o', markersize=3)
    ax1.set_title("Traffic Density (Events per Second)", fontsize=12)
    ax1.set_xlabel("Time (s)")
    ax1.set_ylabel("Count")
    ax1.grid(True, alpha=0.3)

    # 图2: 延迟分布 (验证长尾效应)
    ax2 = fig.add_subplot(gs[0, 1])
    sns.histplot(df['delay'], bins=50, kde=True, color='tab:orange', ax=ax2)
    ax2.set_title("Network Delay Distribution (Log-Normal Check)", fontsize=12)
    ax2.set_xlabel("Delay (s)")
    # 标注出 P99
    p99 = df['delay'].quantile(0.99)
    ax2.axvline(p99, color='red', linestyle='--')
    ax2.text(p99, ax2.get_ylim()[1] * 0.8, f' P99={p99:.2f}s', color='red')

    # 图3: 数据流乱序视图 (Stream View)
    ax3 = fig.add_subplot(gs[1, 0])
    # 画出 event_time 随 send_time 的变化
    # 理想情况是一条直线，乱序会导致点在直线下发抖动
    ax3.scatter(df['send_offset'], df['event_offset'], s=1, alpha=0.5, color='green', label='Event Time')
    # 画出目前为止见到的最大时间 (也就是理想的水位线基准)
    ax3.plot(df['send_offset'], df['max_seen_event'], color='red', linewidth=1, label='Max Seen (Watermark Base)')
    ax3.set_title("Stream Disorder: Arrival vs Event Time", fontsize=12)
    ax3.set_xlabel("Arrival Time (s)")
    ax3.set_ylabel("Event Time (s)")
    ax3.legend()

    # 图4: 水位线权衡曲线 (The Trade-off Curve) - 最重要！
    ax4 = fig.add_subplot(gs[1, 1])
    ax4.plot(thresholds, drop_rates, color='tab:red', linewidth=2)
    ax4.set_title("Watermark Trade-off: Lag Time vs Drop Rate", fontsize=12)
    ax4.set_xlabel("Watermark Lag Setting (s)")
    ax4.set_ylabel("Estimated Data Loss (%)")
    ax4.grid(True, which="both", ls="-")

    # 标记一些关键点，比如 0.5%, 1% 丢包率对应的 Lag
    for target_loss in [1, 5]:
        # 找到最接近该丢包率的 lag
        idx = (np.abs(np.array(drop_rates) - target_loss)).argmin()
        lag_at_loss = thresholds[idx]
        actual_loss = drop_rates[idx]
        ax4.plot(lag_at_loss, actual_loss, 'ko')
        ax4.text(lag_at_loss, actual_loss + 1, f'{actual_loss:.1f}% Loss @ {lag_at_loss:.2f}s')

    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    analyze_data()