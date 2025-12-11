import sys
import os
import time
import argparse
from pyflink.common import Types, WatermarkStrategy, Duration
from pyflink.datastream import StreamExecutionEnvironment, DataStream, OutputTag
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.time import Time
# [关键修改] 导入 SlidingEventTimeWindows
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction, MapFunction, RuntimeContext

# ==================== ⚙️ 全局配置区 (可修改) ====================
PARALLELISM = 3  # Job 整体并行度 (现在是 2)
SERVER_HOST = '172.25.210.154'  # 你的服务端 IP
SERVER_PORT = 9999
WINDOW_SIZE_MS = 2000  # 窗口大小：10秒 (10000ms)
WINDOW_SLIDE_MS = 2000  # 滑动步长：2秒 (2000ms)
LAG = 1000
# =============================================================
EXPERIMENT_NAME = "to-verify-Stu3020Laixin"
# ================= 📝 全局配置与文件初始化 =================
FILE_WINDOW_METRICS = f"/tmp/experiment_logs/{EXPERIMENT_NAME}-experiment_window_stats_slide_SZ{WINDOW_SIZE_MS}-TP{WINDOW_SLIDE_MS}-P{PARALLELISM}.csv"
FILE_LATE_LOG = f"/tmp/experiment_logs/{EXPERIMENT_NAME}-experiment_late_data_slide_SZ{WINDOW_SIZE_MS}-TP{WINDOW_SLIDE_MS}-P{PARALLELISM}.csv"



# 初始化 CSV 文件头 (保持一致，用于写入 Task ID)
def init_files():
    os.makedirs("/tmp/experiment_logs", exist_ok=True)
    with open(FILE_WINDOW_METRICS, 'w') as f:
        f.write("task_id,window_end,trigger_ts,count_actual,count_expected,loss_network,lag_system,watermark_setting\n")
    with open(FILE_LATE_LOG, 'w') as f:
        f.write("task_id,system_ts,event_ts,log_id,content,lag_magnitude\n")


# 定义侧输出流标签 (Side Output Tag) 用于捕获迟到数据
LATE_DATA_TAG = OutputTag("late-data", Types.TUPLE([Types.LONG(), Types.INT(), Types.STRING()]))


# ================= 🔧 核心逻辑类 (RichFunction) =================

class LogTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(value[0])


class AdvancedWindowStats(ProcessWindowFunction):
    """
    高级窗口统计：写入 metrics_window.csv，包含 Task ID
    """

    def __init__(self, watermark_setting):
        self.watermark_setting = watermark_setting
        self.task_id = -1

    def open(self, runtime_context: RuntimeContext):
        self.task_id = runtime_context.get_index_of_this_subtask()
        # 【新增】确保当前机器（无论是Master还是Remote）都有这个文件夹
        os.makedirs(os.path.dirname(FILE_WINDOW_METRICS), exist_ok=True)

    def process(self, key, context, elements):
        current_wm = context.current_watermark()
        window_end = context.window().end
        trigger_ts = time.time() * 1000  # 系统当前时间(ms)

        log_ids = [e[1] for e in elements]
        count_actual = len(elements)

        if count_actual > 0:
            min_id = min(log_ids)
            max_id = max(log_ids)
            # 注意：这里的 count_expected 在滑动窗口下通常不准确，因为数据会重叠
            count_expected = max_id - min_id + 1
            loss_network = count_expected - count_actual
        else:
            count_expected = 0
            loss_network = 0

        lag_system = trigger_ts - window_end

        # 3. 写入文件 (包含 self.task_id)
        try:
            with open(FILE_WINDOW_METRICS, 'a') as f:
                f.write(
                    f"{self.task_id},"
                    f"{window_end},{int(trigger_ts)},{count_actual},{count_expected},{loss_network},{int(lag_system)},{current_wm}\n")
        except Exception as e:
            print(f"Write Window Metrics Error: {e}")

        # 4. 控制台精简打印 (新增 Task ID)
        result = (f"[{self.task_id}-Stu3020Laixin] >>> 🪟 [SLIDING Win {window_end}] [WM={current_wm}] | "
                  f"Count={count_actual} | "
                  f"SysLag={int(lag_system)}ms")  # 简化打印，避免 NetLoss 误导
        yield result


class LateDataLogger(MapFunction):
    """
    处理被丢弃的迟到数据，并写入文件，包含 Task ID
    """

    def __init__(self):
        self.task_id = -1

    def open(self, runtime_context: RuntimeContext):
        self.task_id = runtime_context.get_index_of_this_subtask()

    def map(self, value):
        event_ts, log_id, content = value
        system_ts = time.time() * 1000
        lag_magnitude = system_ts - event_ts

        # 写入文件 (包含 self.task_id)
        try:
            with open(FILE_LATE_LOG, 'a') as f:
                f.write(
                    f"{self.task_id},"
                    f"{int(system_ts)},{event_ts},{log_id},{content},{int(lag_magnitude)}\n")
        except Exception as e:
            print(f"Write Late Metrics Error: {e}")

        # 打印到控制台 (新增 Task ID)
        return f"[{self.task_id}] ⚠️ [LATE DROP] ID={log_id} (Lag={int(lag_magnitude)}ms)"


# ================= 🔧 5. Socket Source 辅助函数 =================
def create_parallel_socket_source(env, host, port, parallelism):
    try:
        j_env = env._j_stream_execution_environment
        # SocketTextStream 是非并行的，P=1
        j_data_stream = j_env.socketTextStream(host, int(port), '\n', 0)
        return DataStream(j_data_stream)

    except Exception as e:
        print(f"❌ Error during Java Gateway call: {e}")
        raise e


# ================= 🔧 6. 数据安全解析类 (来自上次成功运行的版本) =================
class SafeParser(MapFunction):
    def map(self, line):
        line = line.strip()
        if not line:
            return None

        try:
            parts = line.split(',', 2)
            if len(parts) < 3:
                return None

            event_ts = int(parts[0].strip())
            log_id = int(parts[1].strip())
            content = parts[2]

            return (event_ts, log_id, content)

        except ValueError as e:
            # 捕获非法数据，避免 Task 崩溃
            print(f"Skipping record due to bad format (Non-integer field).")
            return None
        except Exception:
            return None


# ================= 🚀 主程序 =================

def run_job(max_lag_ms=LAG, window_size=WINDOW_SIZE_MS, slide_step=WINDOW_SLIDE_MS, parallelism=PARALLELISM):
    # 每次运行前初始化文件
    # Update globals for init_files to use (or pass them down, but init_files uses globals)
    global FILE_WINDOW_METRICS, FILE_LATE_LOG, PARALLELISM
    PARALLELISM = parallelism
    FILE_WINDOW_METRICS = f"/tmp/experiment_logs/{EXPERIMENT_NAME}-experiment_window_stats_slide_SZ{window_size}-TP{slide_step}-P{parallelism}.csv"
    FILE_LATE_LOG = f"/tmp/experiment_logs/{EXPERIMENT_NAME}-experiment_late_data_slide_SZ{window_size}-TP{slide_step}-P{parallelism}.csv"
    
    init_files()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)

    print(f"🚀 Job 启动: Sliding Window {window_size}ms/{slide_step}ms, Par={parallelism}, Lag={max_lag_ms}ms")

    # --- 1. Source (P=1) ---
    ds_raw = create_parallel_socket_source(env, SERVER_HOST, SERVER_PORT, parallelism)

    # 强制分发：Source 端 P=1，rebalance() 会将数据分发给下游 P=PARALLELISM 的 Tasks
    ds_distributed = ds_raw.rebalance()

    # --- 2. 解析 (P=PARALLELISM) ---
    type_info = Types.TUPLE([Types.LONG(), Types.INT(), Types.STRING()])

    # 所有的 Map 操作都在 P=PARALLELISM 上进行
    parsed_stream = ds_distributed \
        .map(lambda line: line.strip(), output_type=Types.STRING()) \
        .map(SafeParser(), output_type=type_info) \
        .set_parallelism(parallelism) \
        .filter(lambda x: x is not None)

    # --- 3. Watermark ---
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_millis(max_lag_ms)) \
        .with_timestamp_assigner(LogTimestampAssigner())

    # --- 4. Window + Side Output ---
    # [核心修改] 使用 SlidingEventTimeWindows
    windowed_stream = parsed_stream \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(lambda x: x[1] % 10 * 1001) \
        .window(SlidingEventTimeWindows.of(Time.milliseconds(window_size),
                                           Time.milliseconds(slide_step))) \
        .side_output_late_data(LATE_DATA_TAG) \
        .process(AdvancedWindowStats(max_lag_ms), Types.STRING())

    # --- 5. 处理主流结果 (打印) ---
    windowed_stream.print().set_parallelism(parallelism)

    # --- 6. 处理迟到数据流 (写入文件) ---
    late_stream = windowed_stream.get_side_output(LATE_DATA_TAG)
    late_stream.map(LateDataLogger()).set_parallelism(parallelism)  # MapFunction 自动继承 output_type

    env.execute(f"Sliding_P{parallelism}_S{window_size / 1000}s_D{slide_step / 1000}s")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--lag', type=int, default=LAG, help='Max Watermark Lag (ms)')
    parser.add_argument('--window_size', type=int, default=WINDOW_SIZE_MS)
    parser.add_argument('--slide_step', type=int, default=WINDOW_SLIDE_MS)
    parser.add_argument('--parallelism', type=int, default=PARALLELISM)
    
    # Flink might pass other arguments, use parse_known_args
    args, unknown = parser.parse_known_args()
    
    run_job(max_lag_ms=args.lag, 
            window_size=args.window_size, 
            slide_step=args.slide_step, 
            parallelism=args.parallelism)