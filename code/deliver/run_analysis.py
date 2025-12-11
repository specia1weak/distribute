import os
import glob
import argparse
from analyze_single_experiment import analyze_experiment

DATA_DIR = r"d:\pyprojects\distribute\data\new"
OUTPUT_DIR = r"d:\pyprojects\distribute\data\analysis"

def main():
    parser = argparse.ArgumentParser(description="运行分布式实验数据分析")
    parser.add_argument("--single", type=str, help="只分析单个实验，提供实验前缀 (例如 3p0.5klag)")
    parser.add_argument("--cutoff", type=int, default=20, help="排除实验最后 N 秒的数据 (默认: 10)")
    args = parser.parse_args()

    # 确保输出目录存在
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    cutoff = args.cutoff

    if args.single:
        # 单个实验模式
        analyze_experiment(DATA_DIR, args.single, OUTPUT_DIR, cutoff_seconds=cutoff)
    else:
        # 扫描所有实验
        # 假设文件名格式是 {prefix}_task-{task_id}.csv
        # 我们查找所有csv，然后提取前缀
        csv_files = glob.glob(os.path.join(DATA_DIR, "*_task-*.csv"))
        prefixes = set()
        for f in csv_files:
            basename = os.path.basename(f)
            # 简单粗暴提取: 分割 "_task-"
            if "_task-" in basename:
                prefix = basename.split("_task-")[0]
                prefixes.add(prefix)
        
        print(f"🔍 发现 {len(prefixes)} 个实验组: {list(prefixes)}")
        for prefix in sorted(prefixes):
            try:
                analyze_experiment(DATA_DIR, prefix, OUTPUT_DIR, cutoff_seconds=cutoff)
            except Exception as e:
                print(f"❌ 分析实验 {prefix} 失败: {e}")

if __name__ == "__main__":
    main()
