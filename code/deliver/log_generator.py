import csv
import random
import math

# ================= 🔧 高级配置区 =================
OUTPUT_FILE = "experiment_data_pro.csv"
TOTAL_LOGS = 20000

# --- 1. 流量模型配置 ---
BASE_RATE = 100  # 基础速率 (条/秒)
BURST_ENABLE = True  # 是否开启流量突发模式
BURST_PERIOD = 20  # 突发周期(秒)，例如每20秒一波高峰
BURST_AMPLITUDE = 2.0  # 突发倍数 (流量变成基础的几倍)

# --- 2. 网络延迟模型 (长尾分布) ---
# 使用对数正态分布 (Log-Normal) 模拟真实网络延迟
# Mu 和 Sigma 是对数域的参数，不是直接的秒数
DELAY_MU = -1.0  # 调整这个值改变平均延迟
DELAY_SIGMA = 0.8  # 调整这个值改变"长尾"程度 (越大，极慢的数据越多)
MIN_DELAY = 0.05  # 物理最小延迟 (50ms)

# --- 3. 异常模拟 ---
DROP_RATE = 0.02  # 丢包率


# ================================================

def generate_realistic_data():
    print(f"🔨 [PRO版] 生成数据: {TOTAL_LOGS}条")
    print(f"   - 流量模型: {'泊松过程 + 周期突发' if BURST_ENABLE else '泊松过程'}")
    print(f"   - 延迟模型: 对数正态分布 (模拟长尾延迟)")

    raw_data = []
    current_event_time = 0.0

    for i in range(1, TOTAL_LOGS + 1):
        # --- 核心改造1: 动态速率 (模拟流量潮汐) ---
        current_rate = BASE_RATE
        if BURST_ENABLE:
            # 使用正弦波模拟流量波动: base * (1 + sin)
            # 这里的 math.pi * 2 * current_event_time / BURST_PERIOD 决定周期
            wave_factor = 0.5 * (1 + math.sin(2 * math.pi * current_event_time / BURST_PERIOD))
            # 让波峰达到 BURST_AMPLITUDE 倍
            current_rate = BASE_RATE * (1 + wave_factor * (BURST_AMPLITUDE - 1))

        # --- 核心改造2: 泊松到达 (Poisson Arrival) ---
        # 事件间隔服从指数分布，这是自然界随机事件的标准模型
        inter_arrival_time = random.expovariate(current_rate)
        current_event_time += inter_arrival_time

        # --- 核心改造3: 长尾延迟 (Log-Normal) ---
        # 绝大多数延迟很低，但偶尔会出现极高的延迟
        network_delay = random.lognormvariate(DELAY_MU, DELAY_SIGMA)
        # 修正过小的延迟 (不能低于物理极限)
        final_delay = max(MIN_DELAY, network_delay)

        # 发送时间
        send_time = current_event_time + final_delay

        # 模拟丢包
        if random.random() < DROP_RATE:
            continue

        raw_data.append({
            "send_offset": send_time,
            "event_offset": current_event_time,
            "id": i,
            "delay": final_delay,  # 记录一下延迟方便分析
            "content": f"Log_{i}"
        })

    # 按发送时间排序 (物理到达顺序)
    sorted_data = sorted(raw_data, key=lambda x: x["send_offset"])

    # 写入文件
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["send_offset", "event_offset", "id", "delay", "content"])
        for row in sorted_data:
            writer.writerow([
                f"{row['send_offset']:.4f}",
                f"{row['event_offset']:.4f}",
                row['id'],
                f"{row['delay']:.4f}",
                row['content']
            ])

    # --- 打印统计信息帮助理解数据 ---
    delays = [d['delay'] for d in raw_data]
    avg_delay = sum(delays) / len(delays)
    max_delay = max(delays)
    print(f"✅ 生成完毕 -> {OUTPUT_FILE}")
    print(f"📊 统计数据:")
    print(f"   - 平均延迟: {avg_delay:.3f}s")
    print(f"   - 最大延迟: {max_delay:.3f}s (这就是那条著名的'迟到'数据)")
    print(f"   - 延迟 > 2s 的占比: {len([d for d in delays if d > 2]) / len(delays) * 100:.2f}%")


if __name__ == "__main__":
    generate_realistic_data()