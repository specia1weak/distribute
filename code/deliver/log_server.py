import socket
import time
import csv
import sys
import threading

from time_utils import high_precision_sleep

# ================= 配置区 =================
DATA_FILE = "experiment_data_pro.csv"
HOST = '0.0.0.0'  # 监听所有网卡，确保 WSL 或外部 Flink 能连上
PORT = 9999
WARMUP_SEC = 5  # 实验开始前的预热/准备时间


# ==========================================

def load_data():
    """读取并解析生成好的数据文件"""
    data_queue = []
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data_queue.append({
                    "send_offset": float(row["send_offset"]),
                    "event_offset": float(row["event_offset"]),
                    "id": int(row["id"]),
                    "content": row["content"]
                })
    except FileNotFoundError:
        print(f"❌ 错误: 找不到文件 {DATA_FILE}，请先运行生成器。")
        sys.exit(1)
    return data_queue


def handle_client(conn, addr, data_queue):
    print(f"🔗 Flink 节点已连接: {addr}")
    print(f"⏳ 准备就绪，{WARMUP_SEC}秒后开始发送数据...")

    high_precision_sleep(WARMUP_SEC)

    # 记录实验开始的基准时间 (T0)
    # 按照你的需求：第一条数据的发送时间应该是 T0 + send_offset
    # 而事件时间戳是 T0 + event_offset
    base_time = time.time()
    print("🚀 实验开始！开始重放日志流...")

    count = 0
    try:
        for row in data_queue:
            # 1. 计算目标发送的物理时间
            target_send_time = base_time + row['send_offset']

            # 2. 精确等待 (Busy-wait 也可以，但 sleep 在毫秒级够用了)
            current_time = time.time()
            sleep_time = target_send_time - current_time

            if sleep_time > 0:
                high_precision_sleep(sleep_time)

            # 3. 构造发送给 Flink 的 Payload
            # 格式: 事件时间戳(ms),日志ID,日志内容
            # 注意：Flink 默认是毫秒时间戳
            event_timestamp = int((base_time + row['event_offset']) * 1000)

            # 组装消息，务必加上换行符 \n，因为 Flink socketTextStream 按行读取
            message = f"{event_timestamp},{row['id']},{row['content']}\n"

            conn.sendall(message.encode('utf-8'))

            # 打印部分日志显示进度
            if count % 50 == 0:
                print(f"   [Sent] Offset={row['send_offset']:.2f}s | EventTS={event_timestamp} | ID={row['id']}")
            count += 1

    except BrokenPipeError:
        print("❌ 客户端(Flink)断开了连接")
    except Exception as e:
        print(f"❌ 发送异常: {e}")
    finally:
        print(f"🏁 发送结束。共发送 {count} 条数据。")
        conn.close()


def start_server():
    data = load_data()
    print(f"✅ 已加载 {len(data)} 条数据，等待 Flink 连接...")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # 允许端口复用，避免程序重启时端口被占
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server.bind((HOST, PORT))
        server.listen(1)
        print(f"🎧 服务端监听在 {HOST}:{PORT}")

        while True:
            # 阻塞等待 Flink 连接
            conn, addr = server.accept()
            # 开启一个线程处理发送，这样主程序可以响应 Ctrl+C
            client_thread = threading.Thread(target=handle_client, args=(conn, addr, data))
            client_thread.start()
            client_thread.join()  # 简单起见，这里同步等待一次实验结束

            print("🔄 等待下一次连接 (或按 Ctrl+C 退出)...")

    except KeyboardInterrupt:
        print("\n🛑 服务端停止")
    finally:
        server.close()


if __name__ == "__main__":
    start_server()