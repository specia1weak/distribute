# pyflink代码的作用，以及如何进行不同参数的实验
实验中通过key_by(lambda x: x[1] % 10 * 1001) 分派来自log_server的数据。由于集群只有3个节点
1. 为什么不用lambda x: x[1]作为key，据我所知，flink会根据key的不同切分多个平行宇宙，假设每一条数据有一个唯一key，那最后所有窗口的接受数据都是1条
2. 为什么不用lambda x: x[1] % 3 作为key，经过实验，flink会根据【】机制分配给三个不同的节点，这很可能导致3个key只有一个Taskmanager在工作，其他干等
3. 为什么不用lambda x: x[1] % 10，经过实验，key 如果是 {0, 1, ..., 10} 这个集合flink的hash算法可能以7:3:0的量分配给Taskmanager导致一个worker干等
4. 为什么使用lambda x: x[1] % 10  * 1001 经过实验，key 如果是 {0, 1001, ..., 10010}，虽然同样只有10个值，貌似每个Taskmanager最终会得到3:3:4的log量是比较理想的状态。
5. 设计socketTextStream为什么强制设定并行度为1，貌似flink的socketTextStream只允许并行度为1，之前我们尝试让每个Taskmanager去连接log_server会导致连接反复断开建立。
6. 