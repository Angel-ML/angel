# Angel 3.4.0 Release Notes

## 一、概述

Angel 3.4.0 是一个以**图计算能力扩充、部署方式扩展和稳定性加固**为主的版本。本版本将 Spark on Angel（SONA）的图算法统一迁移到 `spark-on-angel-mllib`，并新增数十个图算法（连通分量、社区发现、图嵌入、排序、统计特征、链接预测、ANN 检索等），同时引入时间序列、KD-Tree、频繁模式挖掘、异常检测等通用机器学习模块。PS 侧新增 STANDALONE / EMBEDDED 部署模式、指标体系和 FlatBuffers 序列化，并对传输层、快照恢复、内存管理做了大量可靠性修复。

运行环境：JDK 1.8、Scala 2.12、Spark 3.3.x（编译版本 3.3.1）、Hadoop 2.x（兼容 2.9+/2.10）。本版本已在 Hadoop 2.10.2 + Spark 3.3.4 的 YARN 集群（yarn-cluster 模式）上完成 45 个图/嵌入/ANN 算法示例的端到端验证，其中点击率、链接预测等算法与 local 模式结果逐行一致。

> **升级提示**：图算法的包名由 `com.tencent.angel.graph.*` 调整为 `com.tencent.angel.spark.ml.graph.algo.*`，模块依赖方向也发生了变化，详见"主要特性 - 7. 代码组织与兼容性变更"。

## 二、主要特性

### 1. 图算法大幅扩充（spark-on-angel-mllib）

| 类别 | 本版本新增 |
|---|---|
| 连通分量 | SCC、BatchedCC、CompressCC、CutCC |
| 社区发现 | LPAX、LouvainCPM、LouvainDensity、LouvainRepair、LouvainOpt、SeedNeighbors（子图）、Modularity |
| 图嵌入 | BGE、LINE v2 / v3、MetaPath2Vec V1/V2、DocVec、EmbeddingPooling、Word2Vec V1、DeepWalk V4、Node2Vec V5 |
| 排序 / 中心性 | ClusterRank、KCoreDC、KCoreCompress、PPR、WHIndex、MultiAttrHIndex、Intimacy、Cooccurrence / WeightedCooccurrence（biclique）、CommonFriendsRecall、TopRelation、MultiAttrAggregate |
| 统计 / 结构 | 有向三角形计数、有向 LCC、GroupDirectedTriangle、WeightedMotifCount V2/V3、SSSP / MSSP、SSHopLayout、BiDirectionalEdges、NonLinkComFriends |
| 点击率特征 | OneHop、OneHopCom、OneHopWeighted、TwoHopCom、LBSBased |
| 边 / 邻居特征 | EdgeAggregate、FunctionalEdgeAggregate、InteractRate、NeighborVectorDistance |
| 链接预测与索引 | LinkPrediction、PostIndex、PostIndex4Hnsw、ReIndexPlus、String2IntReIndex、DeIndex、RePartition |
| 团挖掘 | KClique0、KCliqueV2 |
| ANN 检索 | IVFHnsw、KMeans-IVFHnsw、召回率评估（EvaluateRecall），与已有的 BruteForce、Hnsw 一同提供 |

### 2. 通用机器学习模块

- **时间序列**：ARIMA、AutoARIMA、AR、EWMA、GARCH、Holt-Winters、ACF / PACF、ADF / KPSS / Ljung-Box 检验、Box-Cox、差分、时序特征提取。
- **树模型**：新增 KD-Tree（KDTTrainer / FPKDTTrainer / KDTPredictor）；GBDT 支持可配置 `storageLevel`、特征采样 `featSampleRatio` 和 dense 格式输入。
- **异常检测与分类**：IsolationForest、LOF、KNN、OutlierTransformer。
- **频繁模式挖掘**：Apriori、FPGrowth、PrefixSpan。
- **特征工程**：约 25 个特征预处理组件（分箱、缩放、缺失值填充、互信息、相关性、采样、切分等），以及中文分词（基于 ansj_seg）、PMML 导出（基于 jpmml-sparkml）、基于 Angel 的 ALS 推荐。
- **在线学习**：FTRL / FtrlFM / AsyncFTRL 改用原地向量运算以降低内存开销；FTRL 支持基于 `ModelContext` 初始化。

### 3. PS 部署模式

- **STANDALONE 模式**：不依赖 YARN / Spark，AM 运行在提交进程中，PS / Worker 以独立 JVM 启动，支持通过 ssh 在多台主机拉起。配套脚本 `angel-cluster-up`、`angel-ps-launch`、`angel-standalone-submit`，配置项为 `angel.standalone.*`。
- **EMBEDDED 模式**：AM 内嵌在调用方进程（如 Spark Driver）中运行。
- **Attach 模式**：Spark 作业可通过 `spark.angel.ps.master` 或 `spark.hadoop.angel.master.host/port` 接入已启动的 PS 集群（`AngelAttachClient`），并提供 `waitPS` / `waitAllPSReady` 等就绪等待接口。

### 4. 可观测性

- 新增 `com.tencent.angel.metrics` 指标体系（`AngelMetricsReporter`、`AngelPSMetrics`、`AngelPSLoad` 等），以及 PS 指标服务、传输层服务端指标和 PS 容器生命周期上报。配置项为 `angel.metrics.*`。

### 5. 图计算底层能力（angel-ps-graph）

- 邻居存储抽象 `Neighbor` / `IntNeighbor`，支持 LONGARRAY / INTARRAY 两种存储，以及 RANDOM / ALIASTABLE / SIMPLEREJECT 三种采样方式。
- 节点特征支持 dense + sparse(field) + sequence 组合结构；`GraphNode` 支持训练 / 验证边、边标签和边时间戳。
- 新增 PSF：带时间的邻居采样、边特征采样、节点 / 边标签读写、边时间戳初始化、Node2Vec V2 路径批量拉取等。
- 动态邻居表改用 `CompressedDynamicLongArray` 分批压缩存储，降低内存占用；支持半精度（FP16）序列化。
- 新增 FlatBuffers 序列化（flatbuffers-java 23.5.26），用于 Embedding 模型输出。

### 6. 传输层与稳定性增强

- 客户端：写空闲心跳、按下次重试时间排序的失败重试队列、用户请求截止时间（`angel.psagent.user.request.deadline.ms`，默认 30 分钟）、请求线程池改为有界队列，满载时快速失败。
- 服务端：自动选择 epoll、TCP_NODELAY、写水位线、空闲连接回收（`angel.netty.matrixtransfer.server.channel.idle.seconds`）、端口绑定重试。
- PSF 调用超时保护：`-Dangel.psf.timeout.ms`（默认 5 分钟），超时后让 Spark task 失败并触发重试，避免作业无限挂起。
- JVM 调优：新增 `angel.{ps,worker}.jvm.overhead.mb`（默认 1024）、`angel.ps.jvm.g1.*`、`angel.{am,ps,worker}.java.opts.append.mode`。

### 7. 代码组织与兼容性变更

- **图算法迁移**：原 `spark-on-angel-graph` 中的算法层整体迁到 `spark-on-angel-mllib`。包名由 `com.tencent.angel.graph.<类别>.<算法>` 改为 `com.tencent.angel.spark.ml.graph.algo.<类别>.<算法>`，其中 `community` 改名为 `communitydetection`，`kclique` 移到 `cliques/kclique`。`spark-on-angel-graph` 只保留底层 PS 模型与公共工具。
- **依赖方向反转**：`spark-on-angel-graph` 不再依赖 `spark-on-angel-mllib`，改为 `spark-on-angel-mllib` 依赖 `spark-on-angel-graph`。使用图算法时需依赖 `spark-on-angel-mllib`。
- **移除**：Leiden、MotifCounting（由 WeightedMotifCount V2/V3 替代）；`ml.util.Utils` 中的 `iteratorBatch`、`runOnTimer` 等工具方法已删除。
- **Kubernetes 恢复**：接回 Master/PS/Worker Pod 提交、调度、attempt 重试及清理链路，发布包包含镜像 Dockerfile 和 entrypoint。已在单节点 kind/Kubernetes 1.31 实测：两个 PS 的矩阵读写与保存加载、Worker 梯度任务、PS Pod 删除后重建及模型恢复、应用资源清理。修复 API URL 省略端口导致的旧 Fabric8 WebSocket Origin 错误，以及镜像 Hadoop 2/3 classpath 混载。算法端到端实测包含原生 LR 训练/保存/加载/两种 batch 预测，以及真实 Spark Kubernetes Driver/两个 Executor 的 PageRank Attach 和 Driver 直启 PS。增加 `angel.kubernetes.client.classpath` 隔离 Spark/Angel 的 Fabric8 版本，修复 Worker 晚任务类和预测末批参数加载。多节点、全算法、远程镜像仓库及安全 Hadoop 场景未验证。
- **默认值变化**：PS 默认 `-Xms=-Xmx` 并开启 AlwaysPreTouch；JVM 非堆余量由 512MB 提高到 1024MB；失败重试退避上限改为 5 秒。
- **示例参数调整**：部分示例参数改名，例如 COPRA 的 `weightIndex` 改为 `weightCol`，PageRank 的 `dataPartitionNum` 改为 `partitionNum`。
- **依赖升级**：PS 与 SONA 使用的 Netty 由 `netty-all-4.1.42` 换为 4.1.74 分模块 jar；`stream` 升级到 2.9.6；jackson 固定为 2.13.4，以匹配 Spark 3.3。

### 8. 示例与测试数据

- cluster 示例新增 70 个，local 示例新增 74 个，另有 65 个机器学习与时序示例；新增公共工具 `ClusterExampleUtils`。
- angel-ps 新增 MixedLR、NFM、PNN 的 JSON 配置及本地示例。
- `data/` 下新增 ann、bc、clickrate、eges、embedding_pooling、karate_club_network、docvec 等测试数据，所有新增示例均可直接运行。

## 三、Bug 修复

### PS 核心与传输层

- 修复 Hadoop 2.9+/2.10 下 YARN 自带的 `ResourceRequest` 比较器比较未设置的 execution type 引发空指针、导致 PS 容器申请失败的问题（`YarnContainerAllocator`）。
- 修复请求被判定为 FATAL 或放弃重试时，channel、直接内存 ByteBuf 和 seqId 映射未释放导致的资源泄露（`MatrixTransportClient`）。
- 修复响应与超时之间的竞态：成功响应可能被超时处理覆盖。
- 修复发送异常时调用 `Thread.stop()` 实际无效的问题，改为走正常的请求失败流程。
- 修复 dispatcher 遇到单次异常就终止整个 worker 的问题。
- 修复 PS 恢复事件被节流丢弃、客户端在 PS 恢复后仍然卡住的问题。
- 修复 `ChannelManager2` 的连接检查线程从未启动，以及 `getChannel` 的并发竞争问题。
- 修复 `MapResponseCache.canMerge` 对重复响应的误判。
- 修复 Master / PS 端口检查后再绑定时被抢占（Address already in use）的问题，改为预留端口并重试。
- 修复 App 超时检测循环空转并重复投递 InternalError 事件的问题。
- 修复 NIO 模式下 `ioRatio` 被硬编码为 70、配置不生效的问题。
- 修复固定 512MB 非堆余量导致容器被 YARN 以 exit 137 杀掉的问题。现在余量可配置，AM 日志会明确提示 137 的原因。
- 修复 libjars 中不带 scheme 的文件名无法解析到 Spark staging 目录的问题（`AngelYarnClient`）。

### PS 存储与快照

- 修复 `ElementElementMapStorage`、`LongElementMapStorage`、`StringElementMapStorage` 的 `deepClone` 遍历了新建的空 map、返回空结果的问题。
- 修复 Map 存储的 `dataSize` 与并发写入 / rehash 竞争时抛空指针、进而导致心跳失败的问题。
- 修复 `UniversalEmbeddingNode.deepClone` 遇到 null 数组时的空指针。
- 修复残缺快照被静默加载为空矩阵的问题。现在会抛出 `CorruptedSnapshotException`，并回退到更早的快照目录。
- 修复 `PartAsyncOptimParam` 序列化长度少计一个 int 字段的问题。

### 图算法

- 修复多处 `psfUpdate` 返回的 Future 没有等待就继续读取 PS、产生时序竞争的问题，涉及 LinkPrediction（邻接表校验失败并卡住）、SLPA、COPRA、KCoreDC、LINE、LINE v3。
- 修复 OneHopWeighted 在未开启负载均衡分区时使用默认 Hash 分区器，导致按列范围切分的 PSF 报 `node id is not in range` 的问题。同时修复其读取接口参数类型不匹配引发的 `ClassCastException`。
- 修复 ClusterRank 的出度矩阵和邻居边数矩阵未指定分区器、同样落入 Hash 分区导致越界的问题；每块列数改为向上取整并保证至少为 1。
- 修复 LINE v3 文本输出格式对字符串 / `IElement` 类型的 key 不输出任何内容，导致结果文件为 0 字节的问题；输出统一使用 UTF-8 编码。
- 修复 ANN `GetSearchResult` 在行或结果为空时保存模型报空指针的问题。
- 修复 `GetNeighbor` 对空邻居的处理，以及加载边数据时含 null 的行导致的异常。

### 机器学习

- 修复 FPGBDT 训练中 `isFeatUsed` 数组长度按已用特征数分配导致的越界问题。

### 示例与部署

- 修复 `cluster/LINEExample` 无条件调用 `setMaster`、在 `spark-submit --master yarn` 下与 Spark 3 冲突的问题；新增的 cluster 示例统一通过 `ClusterExampleUtils.start` 创建 SparkContext。
- 修复 `spark-on-angel-env.sh` 引用发布包中不存在的 `netty-all-4.1.42`、`stream-2.7.0`，导致 PS / Executor 缺少依赖的问题。
- 修复 Word2Vec 示例硬编码本地 native 库路径、EgoNetwork 示例忽略 `srcIndex` / `dstIndex` / `nodeIndex` 参数、MetaPath2Vec 读取节点类型时未过滤空行等问题。
- 修复 Spark 3.3 下传递依赖引入的 jackson 2.7.7 导致 SONA 单元测试中 `MetricsServlet` 初始化失败的问题。
