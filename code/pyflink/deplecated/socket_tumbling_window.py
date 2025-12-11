import sys
import os
import time
import argparse
from pyflink.common import Types, WatermarkStrategy, Duration, Configuration
from pyflink.datastream import StreamExecutionEnvironment, DataStream, OutputTag
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.time import Time
# [关键修改] 导入 TumblingEventTimeWindows
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction, MapFunction, RuntimeContext

# ==================== ⚙️ 全局配置区 (默认值) ====================
PARALLELISM = 3
SERVER_HOST = '172.25.210.154'
SERVER_PORT = 9999
WINDOW_SIZE_MS = 2000 
LAG = 1000
EXPERIMENT_NAME = "to-verify-Stu3020Laixin"

# ================= 📝 文件路径占位符 =================
FILE_WINDOW_METRICS = ""
FILE_LATE_LOG = ""

def init_files():
    os.makedirs("/tmp/experiment_logs", exist_ok=True)
    with open(FILE_WINDOW_METRICS, 'w') as f:
        # 最后一列改为 current_watermark 用于 Debug
        f.write("task_id,window_end,trigger_ts,count_actual,count_expected,loss_network,lag_system,current_wm\n")
    with open(FILE_LATE_LOG, 'w') as f:
        f.write("task_id,system_ts,event_ts,log_id,content,lag_magnitude\n")

LATE_DATA_TAG = OutputTag("late-data", Types.TUPLE([Types.LONG(), Types.INT(), Types.STRING()]))

# ================= 🔧 核心逻辑类 =================

class LogTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(value[0])

class AdvancedWindowStats(ProcessWindowFunction):
    def __init__(self, watermark_setting):
        self.watermark_setting = watermark_setting
        self.task_id = -1

    def open(self, runtime_context: RuntimeContext):
        self.task_id = runtime_context.get_index_of_this_subtask()
        os.makedirs(os.path.dirname(FILE_WINDOW_METRICS), exist_ok=True)

    def process(self, key, context, elements):
        current_wm = context.current_watermark()
        window_end = context.window().end
        trigger_ts = time.time() * 1000

        log_ids = [e[1] for e in elements]
        count_actual = len(elements)

        if count_actual > 0:
            min_id = min(log_ids)
            max_id = max(log_ids)
            count_expected = max_id - min_id + 1 # 滚动窗口内 ID 应该是连续的
            loss_network = count_expected - count_actual
        else:
            count_expected = 0
            loss_network = 0

        lag_system = trigger_ts - window_end

        try:
            with open(FILE_WINDOW_METRICS, 'a') as f:
                # 记录真实 Watermark
                f.write(
                    f"{self.task_id},"
                    f"{window_end},{int(trigger_ts)},{count_actual},{count_expected},{loss_network},{int(lag_system)},{current_wm}\n")
        except Exception as e:
            print(f"Write Window Metrics Error: {e}")

        result = (f"[{self.task_id}-TUMBLING] >>> 🪟 [Win {window_end}] [WM={current_wm}] | "
                  f"Count={count_actual} | "
                  f"SysLag={int(lag_system)}ms")
        yield result

class LateDataLogger(MapFunction):
    def __init__(self):
        self.task_id = -1

    def open(self, runtime_context: RuntimeContext):
        self.task_id = runtime_context.get_index_of_this_subtask()

    def map(self, value):
        event_ts, log_id, content = value
        system_ts = time.time() * 1000
        lag_magnitude = system_ts - event_ts

        try:
            with open(FILE_LATE_LOG, 'a') as f:
                f.write(
                    f"{self.task_id},"
                    f"{int(system_ts)},{event_ts},{log_id},{content},{int(lag_magnitude)}\n")
        except Exception as e:
            pass
        return f"[{self.task_id}] ⚠️ [LATE DROP] ID={log_id}"

# ================= 🔧 辅助函数 =================
def create_parallel_socket_source(env, host, port, parallelism):
    try:
        j_env = env._j_stream_execution_environment
        # SocketTextStream 是非并行的，P=1
        j_data_stream = j_env.socketTextStream(host, int(port), '\n', 0)
        return DataStream(j_data_stream)

    except Exception as e:
        print(f"❌ Error during Java Gateway call: {e}")
        raise e

class SafeParser(MapFunction):
    def map(self, line):
        try:
            parts = line.split(',', 2)
            if len(parts) < 3: return None
            return (int(parts[0]), int(parts[1]), parts[2])
        except:
            return None

# ================= 🚀 主程序 =================

def run_job(max_lag_ms=LAG, window_size=WINDOW_SIZE_MS, parallelism=PARALLELISM):
    global FILE_WINDOW_METRICS, FILE_LATE_LOG
    
    # 滚动窗口不需要 slide_step，只用 window_size
    FILE_WINDOW_METRICS = f"/tmp/experiment_logs/{EXPERIMENT_NAME}-experiment_window_stats_tumbling_SZ{window_size}-P{parallelism}.csv"
    FILE_LATE_LOG = f"/tmp/experiment_logs/{EXPERIMENT_NAME}-experiment_late_data_tumbling_SZ{window_size}-P{parallelism}.csv"
    
    init_files()

    # [优化] 显式设置 Watermark 生成间隔为 200ms
    config = Configuration()
    config.set_string("pipeline.auto-watermark-interval", "200ms")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(parallelism)

    print(f"🚀 Job 启动: Tumbling Window {window_size}ms, Par={parallelism}, Lag={max_lag_ms}ms, WM_Interval=200ms")

    # 1. Source (P=1)
    ds_raw = create_parallel_socket_source(env, SERVER_HOST, SERVER_PORT, parallelism)
    ds_distributed = ds_raw.rebalance() # 关键：分发数据

    # 2. Parse
    type_info = Types.TUPLE([Types.LONG(), Types.INT(), Types.STRING()])
    parsed_stream = ds_distributed \
        .map(lambda line: line.strip(), output_type=Types.STRING()) \
        .map(SafeParser(), output_type=type_info) \
        .set_parallelism(parallelism) \
        .filter(lambda x: x is not None)

    # 3. Watermark And Window
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_millis(max_lag_ms)) \
        .with_timestamp_assigner(LogTimestampAssigner())

    windowed_stream = parsed_stream \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(lambda x: x[1] % 10 * 1001) \
        .window(TumblingEventTimeWindows.of(Time.milliseconds(window_size))) \
        .side_output_late_data(LATE_DATA_TAG) \
        .process(AdvancedWindowStats(max_lag_ms), Types.STRING())

    windowed_stream.print().set_parallelism(parallelism)

    # 4. Late Data
    late_stream = windowed_stream.get_side_output(LATE_DATA_TAG)
    late_stream.map(LateDataLogger()).set_parallelism(parallelism)

    env.execute(f"Tumbling_P{parallelism}_S{window_size}ms")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--lag', type=int, default=LAG)
    parser.add_argument('--window_size', type=int, default=WINDOW_SIZE_MS) # 默认 2000
    parser.add_argument('--parallelism', type=int, default=PARALLELISM)
    
    args, unknown = parser.parse_known_args()
    
    run_job(max_lag_ms=args.lag, 
            window_size=args.window_size, 
            parallelism=args.parallelism)
