import argparse
import os
import glob
from analyze_single_experiment import analyze_experiment

# 配置
DATA_DIR = r"d:\pyprojects\distribute\data\experiment_logs"
OUTPUT_DIR = r"d:\pyprojects\distribute\data\analysis"

def main():
    parser = argparse.ArgumentParser(description="运行分布式实验数据分析")
    parser.add_argument("--single", type=str, help="只分析单个实验，提供实验前缀 (例如 3p0.5klag)")
    parser.add_argument("--cutoff", type=int, default=20, help="排除实验最后 N 秒的数据 (默认: 20)")
    args = parser.parse_args()

    # 获取所有唯一的实验前缀
    # 文件名格式: prefix_task-X.csv
    all_files = glob.glob(os.path.join(DATA_DIR, "*_task-*.csv"))
    prefixes = set()
    for f in all_files:
        filename = os.path.basename(f)
        # 假设 _task- 是分隔符
        if "_task-" in filename:
            prefix = filename.split("_task-")[0]
            prefixes.add(prefix)
    
    print(f"🔍 发现 {len(prefixes)} 个实验组: {sorted(list(prefixes))}")

    if args.single:
        print(f"🎯 单独分析: {args.single}")
        analyze_experiment(DATA_DIR, args.single, OUTPUT_DIR, cutoff_seconds=args.cutoff)
    else:
        print(f"🚀 批量分析所有实验...")
        for prefix in sorted(prefixes):
            analyze_experiment(DATA_DIR, prefix, OUTPUT_DIR, cutoff_seconds=args.cutoff)

if __name__ == "__main__":
    main()
