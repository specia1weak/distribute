# G. Flink 水位线机制分析实验报告

## 1\. 研究目的

探究Flink水位线延迟时间对窗口触发与结果准确性的影响。

## 2\. 研究内容

1.  **基础研究**：深入理解Flink的水位线（Watermark）机制，探究水位线延迟时间对窗口触发延迟和
结果计算准确度的影响。进一步研究在乱序数据场景下，如何通过合理设置水位线延迟时间，在延
迟与计算准确性之间取得平衡。
<img width="874" height="429" alt="image" src="https://github.com/user-attachments/assets/1c68fd04-2179-42b0-bc04-92f0319092c9" />

3.  **迟到数据量化分析**：利用 Flink 侧输出流（Side Output）机制，捕获并统计被窗口丢弃的迟到数据，量化不同水位线设置下的“数据丢失成本”，为生产环境的参数调优提供数据支撑。

## 3\. 实验
-----
### 3.1 实验环境

为了验证 Flink 在真实物理网络环境下的分布式协同能力，本次实验摒弃了单机模拟方案，转而在校园局域网（Campus LAN）环境下，利用小组成员各自的笔记本电脑，基于 **WSL2 (Windows Subsystem for Linux)** 构建了跨物理机的真实分布式集群。
  * **集群拓扑架构**：
      * **部署模式**：Standalone Cluster 模式。
      * **节点构成**：3 台物理机（Physical Nodes）组成的完全分布式环境。
      * **网络环境**：校园网局域网环境（同一子网网段），节点间通过 SSH 互通，平均网络延迟 (Ping) 约 5-8ms。
* **硬件配置详情**：

  | 节点角色                                      | 宿主设备       | 宿主配置 (CPU/RAM)         | 运行环境                | 分配资源 (分配给FlinkJVM的内存) |
  |:------------------------------------------|:-----------|:-----------------------|:--------------------|:----------------------|
  | **Master / Node-01**<br>(JobManager + TM) | Windows笔记本 | AMD Ryzen 7 / 32GB     | WSL2 (Ubuntu 22.04) | 4GB                   |
  | **Worker / Node-02**<br>(TaskManager)     | Windows笔记本 | Intel i7-12700H / 16GB | WSL2 (Ubuntu 20.04) | 4GB                   |
  | **Worker / Node-03**<br>(TaskManager)     | Windows笔记本 | Intel i5-1135G7 / 16GB | WSL2 (Ubuntu 20.04) | 4GB                   |
* | 日志流发送服务器         | Mac笔记本     | M4 / 16GB              | Macos               | -                     |
    * **存储**：各节点均配备 NVMe SSD，确保高吞吐数据落地。
s
  * **软件配置**：
      * **子系统 (Guest OS)**：WSL 2 (Ubuntu 20.04 LTS / 22.04 LTS)。
      * **宿主操作系统**：Windows 10/11 专业版。
      * **JDK 版本**：OpenJDK 11.0.19。
      * **Flink 版本**：Apache Flink 1.19.0。
-----

### 3.2 实验负载
* **数据集 (Dataset)**：
    本次实验，采用自定义 Python 脚本（`experiment_generator.py`）构建高保真模拟数据流，通过数学模型还原真实物理网络的不可靠性。数据生成逻辑包含以下核心特征：
    1.  **基于物理规律的流量模型**：
        * **泊松到达（Poisson Process）**：代码使用 `random.expovariate` 生成事件间隔，模拟真实场景下用户请求的随机到达特性。
        * **周期性潮汐突发**：在基础速率（100 ops）之上叠加 **正弦波（Sinusoidal Wave）** 扰动。设定突发周期为 **20秒**，波峰流量放大至基础值的 **2.0倍**，用于测试 Flink 在流量洪峰下的背压处理与窗口计算能力。

    2.  **长尾分布的乱序机制**：
        * **网络延迟建模**：采用 **对数正态分布（Log-Normal Distribution, $\mu=-1.0, \sigma=0.8$）** 替代均匀分布来模拟网络延迟。该模型通过数学手段复现了网络传输中的“长尾效应”——即绝大多数数据延迟极低（50ms左右），但偶发极高延迟（>2000ms）的数据包。
        * **物理乱序注入**：生成器计算 `send_time = event_time + delay`，并最终按照 `send_time` 对数据进行全局排序，从而在数据流中制造了严酷且真实的**事件时间乱序**。

    3.  **数据规模**：
        单次实验样本量为 **20,000 条**，包含约 2% 的随机丢包噪声，确保数据集覆盖平稳期、突发期以及各种程度的乱序场景。
        
    
    ![实验数据](static/data_distribute/distribute.png)
  
  * **工作负载 (Workloads)**：
      实验设计了两组 PyFlink 作业以全面评估不同窗口类型下的水位线行为：
      1.  **滑动窗口作业 (`socket_slide_winv2.py`)**：
          *   **配置**：窗口大小 **2000ms**，滑动步长 **1000ms**。
          *   **目的**：测试在窗口重叠场景下，水位线延迟对连续结果输出平滑性的影响。
      2.  **滚动窗口作业 (`socket_tunmling.py`)**：
          *   **配置**：窗口大小 **2000ms** (非重叠)。
          *   **目的**：作为基准对照组，量化固定时间片内的绝对丢包率与延迟。
      
      **核心处理逻辑**：
      *   **数据接入**：通过 `socketTextStream` 接收 Python 生成器发出的模拟流量 (`Parallelism=3`).
      *   **水位线策略**：使用 `BoundedOutOfOrdernessTimestampExtractor`，Lag 参数动态配置 (0ms - 5000ms)。
      *   **监控埋点**：自定义 `ProcessWindowFunction` 计算 Count/Loss/SystemLag，并利用 **Side Output** 机制捕获所有被丢弃的 Late Event。

### 3.3 实验步骤

#### 实验架构图

为了复现真实的流处理压测场景，我们设计了 "离线生成 - 在线重放" 的实验架构：

![process](static/process/process.png)
#### 步骤 1：PyFlink 分布式环境准备

在 3 台物理机（基于 WSL2）上构建 Standalone 模式的 Flink 集群。
<img width="1394" height="401" alt="image" src="https://github.com/user-attachments/assets/4af1bdda-955e-40b7-b4c9-d92e506996f4" />

1.  **节点角色分配与配置**：
      * **Master (Node-01)**: 运行 `JobManager`
      * **Workers (Node-01, 02, 03)**: 各运行一个 `TaskManager`，组成并行度为 3 的计算资源池。
2.  **运行在WSL的节点配置**
    由于实验在3台windows的WSL2环境上运行，在环境配置比起服务器会有很多关键步骤需要额外说明。
    <img width="1405" height="366" alt="image" src="https://github.com/user-attachments/assets/54a13f74-80a7-4dda-98fe-b5ba8248dfc1" />

    WSL不能直接被其他节点访问，需要配置windows的端口转发规则才能让外界访问到内部的WSL。相关配置如图，关键点包括：
    - 统一jobmanager.rpc.address 为flink-master。在master节点修改hosts文件将flink-master映射到localhost，其他节点将flink-master映射到实际ip。
    - 统一python.executable为固定路径 /opt/pyflink_env/bin/python，如果三个节点python环境在不同路径，需要使用ln -s命令创建软连接到同一路径，否则不能运行。
    - 固定metrics.internal.query-service.port到一个端口，由于flink默认将其设置为随机端口，导致随机端口可能不在WSL的端口转发内而无法通信。

![config.png](static/env/config.png)
3.  **集群启动**：
    在 Master 节点执行启动脚本：
    ```bash
    ./bin/start-cluster.sh
    ```
4.  **环境验证**：
    访问 Flink Web UI，确认 三个TaskManager 状态。
![workers](static/workers.png)
   > 截图说明：Flink Web UI (Dashboard) 的 Task Managers 页面。图中应清晰显示每个节点的ip，以及通信端口，slots数量*

#### 步骤 2：数据源与集群实验关键配置

本实验为了在 Python 环境下获得尽可能高的观测精度，对网络通信与 PyFlink 运行时进行了深度定制。

1.  **Socket 数据源网络配置**：
      * **乱序日志生成**：预先使用`log_generator.py`生成固定的数据，作为每次实验的数据来源。
      * **服务端**：在额外独立的节点上运行`log_server.py` 绑定至 `0.0.0.0:9999`，负责为flink集群提供反复实验并相同的数据源。
      * **时间同步**：各 Worker 节点通过 TCP 连接 Master 的 `9998` 端口，计算 RTT 并获取 `GLOBAL_TIME_OFFSET`，消除物理机系统时钟差异对延迟计算的影响。
      * <img width="988" height="539" alt="image" src="https://github.com/user-attachments/assets/fc21fcc4-fcb7-4710-a742-387d0b1b186e" />


2.  **PyFlink 关键配置（对实验结果影响显著）**：
    由于 PyFlink 涉及 JVM 与 Python VM 之间的跨进程通信，默认配置偏向吞吐量而牺牲了延迟。为了准确观测水位线效果，我们在代码中强制覆盖了以下配置：

      * **水位线生成频率 (`pipeline.auto-watermark-interval`)**：
          * *默认值*：200ms
          * *实验设定*：**10ms**
          * *目的*：确保 Watermark 能紧跟数据流产生，避免因 Watermark 生成滞后导致窗口触发延迟，从而干扰对“真实延迟”的测量。
      * **跨语言包大小 (`python.fn-execution.bundle.size/time`)**：
          * *默认值*：1000ms / 1000条
          * *实验设定*：**10ms / 1条**
          * *目的*：PyFlink 默认会攒批处理以减少 JNI 调用开销。本实验强制减小 Bundle 大小，使 Python UDF（如窗口处理函数）能实时响应，避免数据在缓冲区滞留。
          * <img width="1157" height="631" alt="image" src="https://github.com/user-attachments/assets/6ef91bfe-7a41-4cd9-9a5a-12e953a9f2dd" />


#### 步骤 3：作业执行与变量控制

通过 Flink CLI 提交 PyFlink 作业，分别进行滚动窗口与滑动窗口的对照实验。

1.  **实验组 A：滚动窗口 (Tumbling Window)**
    固定窗口大小 2000ms，控制变量为水位线延迟 (`--lag`)。

    ```bash
    # 示例：提交 Lag=1500ms 的实验任务
    ./bin/flink run -py socket_tunmling.py \
        --lag 1500 \
        --window_size 2000 \
        --parallelism 3
    ```

    *执行 0ms, 1500ms, 3000ms, 5000ms 四组对照实验。*

2.  **实验组 B：滑动窗口 (Sliding Window)**
    引入窗口重叠（Slide=1000ms），测试高负载下的丢包表现。

    ```bash
    ./bin/flink run -py socket_slide_winv2.py \
        --lag 1500 \
        --window_size 2000 \
        --window_slide 1000
    ```

#### 步骤 4：运行过程与过程截图

任务面板里面，可以查看每个Taskmanager的工作状态，处理与接受的数据量等信息。
![working.png](static/working/working.png)

可以查看每个Taskmanager的Stdout，如图每行的输出都记录了一个窗口触发的行为，
也能看到触发这个窗口的水位线、窗口的事件量。这里展示172.25.89.156的日志信息。
这个节点被分配了一个并行度，负责Task2的任务，所以每行以`2>`开头。

![stdout.png](static/working/stdout.png)
> 每一行以**Stu3020Laixin**学号姓名打印的形式来表明实验的真实性。

运行完毕回到主界面，可以看到下方记录了任务的完成状态，消耗时间。
![finish.png](static/working/finish.png)



实验数据分散存储在三个节点的本地文件系统中，需进行聚合分析。
1.  **日志收集**：
    作业结束后，将各 Worker 节点 `/tmp/experiment_logs/` 目录下的 CSV 文件（`_metrics.csv` 和 `_trace.csv`）回传至分析机。
2.  **数据清洗与合并**：
    合并 3 个并行 Subtask 的数据，按照 `window_end` 对齐窗口统计信息。
3.  **可视化生成**：
    运行 `code/analyze` 下的相关分析代码生成评估图表：
      * **延迟-丢包权衡曲线 (Trade-off Curve)**：分析不同 Lag 设置下，系统平均延迟与丢包率的关系。
      * **流量密度图**：验证正弦波流量模型是否生效。
      * **迟到数据分布图**：通过 Trace 日志，精确定位迟到数据的产生时刻与滞后时长。





### 3.4 实验结果与分析

#### 3.4.1 核心指标概览

基于3节点物理集群的实验结果，我们将不同水位线延迟（Lag）下的核心指标汇总如下：

**表 1：Tumbling Window (2s) 实验结果汇总**

| Lag 配置 (s) | 窗口类型 | 丢弃率 (Drop Rate) | 平均处理延迟 (Process Latency) | 窗口触发延迟 (Trigger Latency) | 结果准确性评价 |
| :---: | :---: |:---------------:|:--------------:|:---:| :--- |
| **0.0** | Tumbling |   **13.27%**    |   **1.57s**    | **0.49s** | **极差** (严重失真) |
| **0.5** | Tumbling |    **4.17%**    |     1.95s      |   0.96s   | **较差** (仍有明显丢失) |
| **1.0** | Tumbling |    **1.91%**    |     2.58s      |   1.58s   | **一般** (勉强可用) |
| **1.5** | Tumbling |    **0.76%**    |     3.12s      |   2.10s   | **良好** (平衡点) |
| **2.0** | Tumbling |    **0.43%**    |     3.51s      |   2.52s   | **优秀** (高准确性) |
| **2.5** | Tumbling |    **0.18%**    |     3.98s      |   2.98s   | **极佳** (接近无损) |
| **3.0** | Tumbling |    **0.11%**    |     4.47s      |   3.47s   | **完美** (冗余等待) |
| **5.0** | Tumbling |    **0.00%**    |     6.39s      |   5.39s   | 延迟过高 |

> *注1：平均延迟已剔除最后 50s 数据以消除尾部效应干扰。*
> *注2：窗口触发延迟 = 系统触发时间 - 窗口结束时间。理论上应约等于 Lag。*


#### 3.4.2 丢弃率趋势分析

我们首先关注水位线对数据完整性的影响。下图展示了不同实验组（滚动窗口/滑动窗口）丢弃率随 Lag 变化的趋势：

![DropRate](data/conclusion/drop_rate_vs_lag.png)

*   **极速下降**：当 Lag 从 0s 增加到 1s 时，丢弃率呈现指数级下降（33% -> 0.01%）。这表明绝大多数乱序数据都集中在 1秒 以内的延迟区间。
*   **长尾收敛**：Lag 从 1s 到 2s 的区间内，丢包率从 0.01% 收敛至 0%。这对应了我们在生成器中设置的长尾分布特性。

#### 3.4.3 Trade-off 权衡分析 (Sweet Spot)

流计算的核心在于寻找 **低延迟** 与 **高准确性** 之间的平衡点。我们将 Tumbling Window 组的各项指标绘制在双轴图中：

![Tradeoff](data/conclusion/tradeoff_analysis.png)

*   **红色曲线 (Drop Rate)**：代表准确性损失。在 Lag < 1s 时极高，随后迅速归零。
*   **蓝色虚线 (Process Latency)**：**平均处理延迟**。即数据从生成到被窗口处理完毕的总耗时。它随着 Lag 线性增加。
*   **紫色点划线 (Trigger Latency)**：**窗口触发延迟** (New)。这是 Flink 内部机制的体现，它几乎完美地等于 `Lag + 系统开销(约0.4s)`。
    *   可以看出，处理延迟 ≈ 触发延迟 + 1.0s。这多出的 1.0s 正是 2s 窗口的平均等待时间（半个窗口长度）。这严谨地验证了 Flink 事件时间窗口的触发逻辑。

*   **最佳平衡点 (Sweet Spot)**：
    如图所示，**Lag = 1.0s - 1.5s** 是本实验环境下的最佳配置区间。在此区间内，我们能以可接受的延迟成本，换取 98% 以上的数据完整性。

#### 3.5 实验反思：微观视角下的系统挑战

本次实验特意选择了**毫秒级低延迟**（Window=2s, Lag=0-5s）的场景设定，而非生产环境中常见的分钟/小时级窗口。这一设计看似增加了实验难度，实则为我们深入理解分布式系统的“微观行为”提供了绝佳的放大镜：

1.  **“噪声”被放大为“信号”**：
    在大窗口（如 1小时）场景下，200ms 的系统开销或时钟偏移几乎可以忽略不计。但在本实验的 2s 窗口下，**200ms 的误差即占比 10%**。这迫使我们必须正视那些通常被掩盖的技术细节：
    *   **PyFlink Bundle 机制**：我们发现默认 1s 的 buffer 会直接导致实验数据失真，从而学会了通过 `python.fn-execution.bundle.size=1` 来强制实时处理。
    *   **水位线生成周期**：被迫将 `auto-watermark-interval` 从 200ms 调优至 10ms，以避免触发延迟。

2.  **物理时钟的各种挑战**：
    由于无法依赖单机系统时间，我们直面了 **分布式时钟同步** 这一经典难题。实验中观测到的 "Negative Latency" (负延迟) 现象，直接推动我们实现了基于 RTT 的逻辑时钟校准方案 (`calibrate_data.py`)，这是在单机模拟实验中永远无法体会到的宝贵经验。

3.  **系统与业务的博弈**：
    通过微观数据的波动，我们深刻体会到了 **系统开销（System Overheard）** 与 **业务准确性** 之间的拉锯战。每一毫秒的延迟降低，都需要底层序列化、网络传输、进程调度的紧密配合。

这种“显微镜式”的实验设计，虽然带来了更多的数据清洗工作与排查调整（Troubleshooting），但极大地锻炼了我们在分布式环境下的问题定位能力。

## 4. 结论

通过本次实验，我们得出以下核心结论：

1.  **不存在绝对完美的 Watermark 设置**：水位线机制本质上是在 Trade-off（权衡）。低延迟必然伴随高丢包风险，高准确性必然导致结果产出滞后。
2.  **数据特征决定参数阈值**：本实验测得 1.5s 为该负载下的最优参数。在生产环境中，应先通过 Metrics 监控数据流的乱序分布（如 P99 乱序时间），再依据此设定 Watermark，而不是盲目设置。
## 5\. 分工

| 成员姓名     | 贡献度 | 具体工作内容                                                                                  |
|:---------| :--- |:----------------------------------------------------------------------------------------|
| **赖鑫**   | **35%** | 负责实验整体架构设计，搭建3节点 Flink 集群环境，编写Pyflink核心代码(Watermark策略与侧输出流逻辑)；撰写实验报告主体与结果分析。 |
| **陆锦云**  | 25% | 负责编写乱序数据生成脚本，控制数据分布特征；协助进行多组对照实验的数据记录。                                       |
| **于海洋**  | 20% | 搭建 3节点 Flink 集群环境，负责实验数据的可视化处理；整理实验截图与日志文件;绘制 PPT。                   |
| **徐治平**  | 20% | 搭建3节点 Flink集群环境，对实验报告进行排版与校对，校对PPT，录制讲解视频。                                              |