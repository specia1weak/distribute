import sys
import os
import time
import argparse
import socket
import struct
from pyflink.common import Types, WatermarkStrategy, Duration, Configuration
from pyflink.datastream import StreamExecutionEnvironment, DataStream, OutputTag
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.time import Time
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction, MapFunction, RuntimeContext

# ==================== ⚙️ 全局配置区 ====================
PARALLELISM = 3
SERVER_HOST = '192.168.6.205'
SERVER_PORT = 9999
WINDOW_SIZE_MS = 2000
WINDOW_SLIDE_MS = 1000
LAG = 1500
EXPERIMENT_NAME = "slide21k-1.5klag"

# ================= 📝 全局变量 =================
GLOBAL_TIME_OFFSET = 0.0
BASE_LOG_DIR = "/tmp/experiment_logs"
# [修正] 确保 Tag 定义在最外层
LATE_DATA_TAG = OutputTag("late-data", Types.TUPLE([Types.LONG(), Types.INT(), Types.STRING()]))


# ================= 🔧 时间同步函数 =================
def sync_time_with_master(master_ip, port=9998):
    offsets = []
    # print(f"🔄 [Sync] 正在与 Master ({master_ip}) 同步时钟...")
    for _ in range(5):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.settimeout(2)
            t1 = time.time()
            client.connect((master_ip, port))
            data = client.recv(8)
            t_server = struct.unpack('!d', data)[0]
            t2 = time.time()
            client.close()
            rtt = t2 - t1
            latency = rtt / 2
            offset = t_server + latency - t2
            offsets.append(offset)
            time.sleep(0.1)
        except Exception as e:
            # print(f"⚠️ 同步失败: {e}")
            return 0.0

    if len(offsets) > 2:
        offsets.remove(max(offsets))
        offsets.remove(min(offsets))

    avg_offset = sum(offsets) / len(offsets)
    # print(f"✅ [Sync] 同步完成! 本机偏移量: {avg_offset:.6f}s")
    return avg_offset


def get_synced_time():
    return time.time() + GLOBAL_TIME_OFFSET


# ================= 🔧 核心逻辑类 =================

class LogTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(value[0])


class AdvancedWindowStats(ProcessWindowFunction):
    # [修正] 确保 init 接收 slide 参数
    def __init__(self, watermark_setting, window_size, window_slide):
        self.watermark_setting = watermark_setting
        self.window_size = window_size
        self.window_slide = window_slide
        self.task_id = -1
        self.file_path_metrics = None
        self.file_path_trace = None

    def open(self, runtime_context: RuntimeContext):
        self.task_id = runtime_context.get_index_of_this_subtask()

        base_name = f"{EXPERIMENT_NAME}_Sliding_SZ{self.window_size}_SL{self.window_slide}_P{PARALLELISM}_task-{self.task_id}"
        self.file_path_metrics = os.path.join(BASE_LOG_DIR, f"{base_name}_metrics.csv")
        self.file_path_trace = os.path.join(BASE_LOG_DIR, f"{base_name}_trace.csv")

        try:
            os.makedirs(BASE_LOG_DIR, exist_ok=True)
        except:
            pass

        global GLOBAL_TIME_OFFSET
        GLOBAL_TIME_OFFSET = sync_time_with_master(SERVER_HOST, 9998)

        if not os.path.exists(self.file_path_metrics):
            try:
                with open(self.file_path_metrics, 'w') as f:
                    f.write(
                        "task_id,window_end,trigger_ts,count_actual,count_expected,loss_network,lag_system,current_wm\n")
            except:
                pass

        if not os.path.exists(self.file_path_trace):
            try:
                with open(self.file_path_trace, 'w') as f:
                    f.write("task_id,sys_ts,event_ts,log_id,content,status,window_end\n")
            except:
                pass

    def process(self, key, context, elements):
        current_wm = context.current_watermark()
        window_end = context.window().end
        trigger_ts = get_synced_time() * 1000

        log_ids = [e[1] for e in elements]
        count_actual = len(elements)

        if count_actual > 0:
            min_id = min(log_ids)
            max_id = max(log_ids)
            count_expected = max_id - min_id + 1
            loss_network = count_expected - count_actual
        else:
            count_expected = 0
            loss_network = 0

        lag_system = trigger_ts - window_end

        try:
            with open(self.file_path_metrics, 'a') as f:
                f.write(
                    f"{self.task_id},"
                    f"{window_end},{int(trigger_ts)},{count_actual},{count_expected},{loss_network},{int(lag_system)},{current_wm}\n")
        except Exception as e:
            print(f"Write Metrics Error: {e}")

        try:
            with open(self.file_path_trace, 'a') as f:
                lines = []
                for e in elements:
                    line = f"{self.task_id},{int(trigger_ts)},{e[0]},{e[1]},{e[2]},NORMAL,{window_end}\n"
                    lines.append(line)
                f.writelines(lines)
        except Exception as e:
            print(f"Write Trace Error: {e}")

        result = (f"[{self.task_id}-SLIDING] >>> 🪟 [Win {window_end}] [WM={current_wm}] | Count={count_actual}")
        yield result


class LateDataLogger(MapFunction):
    # [修正] 确保 init 参数与调用处一致
    def __init__(self, window_size, window_slide):
        self.task_id = -1
        self.file_path_trace = None
        self.window_size = window_size
        self.window_slide = window_slide

    def open(self, runtime_context: RuntimeContext):
        self.task_id = runtime_context.get_index_of_this_subtask()

        base_name = f"{EXPERIMENT_NAME}_Sliding_SZ{self.window_size}_SL{self.window_slide}_P{PARALLELISM}_task-{self.task_id}"
        self.file_path_trace = os.path.join(BASE_LOG_DIR, f"{base_name}_trace.csv")

        try:
            os.makedirs(BASE_LOG_DIR, exist_ok=True)
        except:
            pass

        global GLOBAL_TIME_OFFSET
        GLOBAL_TIME_OFFSET = sync_time_with_master(SERVER_HOST, 9998)

        if not os.path.exists(self.file_path_trace):
            try:
                with open(self.file_path_trace, 'w') as f:
                    f.write("task_id,sys_ts,event_ts,log_id,content,status,window_end\n")
            except:
                pass

    def map(self, value):
        event_ts, log_id, content = value
        system_ts = get_synced_time() * 1000

        try:
            with open(self.file_path_trace, 'a') as f:
                f.write(
                    f"{self.task_id},{int(system_ts)},{event_ts},{log_id},{content},LATE,-1\n")
        except Exception as e:
            pass
        return f"[{self.task_id}] ⚠️ [LATE DROP] ID={log_id}"


# ================= 🔧 辅助函数 =================
def create_parallel_socket_source(env, host, port, parallelism):
    try:
        j_env = env._j_stream_execution_environment
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

# [修正] 这里补上了 window_slide 参数，之前这里漏了导致报错
def run_job(max_lag_ms=LAG, window_size=WINDOW_SIZE_MS, window_slide=WINDOW_SLIDE_MS, parallelism=PARALLELISM):
    config = Configuration()
    config.set_string("pipeline.auto-watermark-interval", "10ms")
    config.set_string("python.fn-execution.bundle.time", "10")
    config.set_string("python.fn-execution.bundle.size", "1")

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_buffer_timeout(10)
    env.set_parallelism(parallelism)

    print(f"🚀 Job 启动: Sliding Window Size={window_size}ms, Slide={window_slide}ms, Par={parallelism}")

    ds_raw = create_parallel_socket_source(env, SERVER_HOST, SERVER_PORT, parallelism)
    ds_distributed = ds_raw.rebalance()

    type_info = Types.TUPLE([Types.LONG(), Types.INT(), Types.STRING()])
    parsed_stream = ds_distributed \
        .map(lambda line: line.strip(), output_type=Types.STRING()) \
        .map(SafeParser(), output_type=type_info) \
        .set_parallelism(parallelism) \
        .filter(lambda x: x is not None)

    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_millis(max_lag_ms)) \
        .with_timestamp_assigner(LogTimestampAssigner())

    windowed_stream = parsed_stream \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(lambda x: x[1] % parallelism * 1001) \
        .window(SlidingEventTimeWindows.of(Time.milliseconds(window_size), Time.milliseconds(window_slide))) \
        .side_output_late_data(LATE_DATA_TAG) \
        .process(AdvancedWindowStats(max_lag_ms, window_size, window_slide), Types.STRING())

    windowed_stream.print().set_parallelism(parallelism)

    late_stream = windowed_stream.get_side_output(LATE_DATA_TAG)
    late_stream.map(LateDataLogger(window_size, window_slide)).set_parallelism(parallelism)

    env.execute(f"Sliding_P{parallelism}_S{window_size}_Slide{window_slide}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--lag', type=int, default=LAG)
    parser.add_argument('--window_size', type=int, default=WINDOW_SIZE_MS)
    # 确保这里也能接收参数
    parser.add_argument('--window_slide', type=int, default=WINDOW_SLIDE_MS)
    parser.add_argument('--parallelism', type=int, default=PARALLELISM)

    args, unknown = parser.parse_known_args()

    run_job(max_lag_ms=args.lag,
            window_size=args.window_size,
            window_slide=args.window_slide,
            parallelism=args.parallelism)