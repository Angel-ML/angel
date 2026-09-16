# HNSW

## 1. 算法介绍

> HNSW（Hierarchical Navigable Small World）是基于图的一种近似最近邻搜索算法（Approximate Nearest
>
Neighbor，ANN），通过构建具有分层结构、可导航的、类似跳表结构的搜索图来快速地在大规模高维向量数据中召回topK邻居。HNSW算法出自于论文《[Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs](https://arxiv.org/abs/1603.09320)》。

## 2. 运行

#### 算法IO参数

- vectorPath：hdfs路径，构建图向量数据
- queryPath：hdfs路径，指定的查询向量数据，若此配置为空且isInternal（见算法参数说明）为true，vectorPath作为查询向量数据
- outputPath：hdfs路径，输出查询向量数据结果
- itemSep：ID列与向量数据列之间的分隔符(space, comma, tab, colon), 默认为colon
- vecSep：向量数据列内部分隔符(space, comma, tab, colon), 默认为space
- saveItemSep：保存ID列与最近邻结果之间的分隔符(space, comma, tab), 默认为tab

#### 算法参数

- psPartitionNum：模型分区个数，最好是parameter server个数的整数倍，让每个ps承载的分区数量相等，让每个PS负载尽量均衡,
  数据量大的话推荐500以上
- partitionNum：输入RDD数据分区大小，一般设为spark executor个数乘以executor core数的3-4倍
-
storageLevel：RDD存储级别（[可选值参考](https://spark.apache.org/docs/0.8.1/api/core/org/apache/spark/storage/StorageLevel$.html)
），默认为MEMORY_ONLY
- batchSize：查询向量数据单次计算最近邻居的batch大小，每个batch处理涉及PS相似度数据传输和spark
  executor最近邻搜索计算，配置可依据运行资源配置、集群带宽、向量维度适当调整
- topK：查询向量数据计算最近邻居个数
- isInternal：false表示使用queryPath的ID作为query，此时queryPath必须配置；true表示queryPath配置不为空，则使用queryPath的ID作为query，否则使vectorPath的ID
- distanceFunction：计算向量之间距离时使用的距离函数，目前支持的距离有四种：cosine-distance、l1-distance、l2-distance、jaccard-distance
- sheetsNum：queries的并发度，并发度越高计算越快，但是相对消耗的资源越大，根据资源情况设置
- queryPartitionNum：查询向量RDD数据分区大小，内存资源有限可适当增大数值
- cpDir：RDD数据checkpoint hdfs地址
- ef：查询向量数据计算最近邻居候选集合的大小，ef越大，越精确，但在分布式场景中，最终候选集合为ef*part num，一般情况下默认即可
- efConstruction：构图候选集合的大小，影响构图的效果，同ef，一般情况下默认即可
- M：构图时每个点需要连接的边数，M越大，构图效果越好，但是存储消耗的资源以及查询的时间开销也会增大，无特殊精度要求，不需要特别大
- maxM：第0层外构图每个点最大连接的边数，限制图中点连边的数量，不至于过大
- maxM0：第0层构图每个点最大连接的边数，限制图中点连边的数量，不至于过大
- mL：构图时计算每个点所在层数的的随机种子，越小层数越多，控制层数，一般情况下默认即可

#### 资源参数

- Angel PS个数和内存大小：ps.instance与ps.memory的乘积是ps总的配置内存。为了保证Angel不挂掉，需要配置模型大小两倍左右的内存。对于PageRank来说，模型大小的计算公式为：
  节点数 * 3 * 4 Byte，据此可以估算不同规模的Graph输入下需要配置的ps内存大小
- Spark的资源配置：num-executors与executor-memory的乘积是executors总的配置内存，最好能存下2倍的输入数据。
  如果内存紧张，1倍也是可以接受的，但是相对会慢一点。 比如说100亿的边集大概有160G大小， 20G * 20 的配置是足够的。
  在资源实在紧张的情况下， 尝试加大分区数目！

#### 任务提交示例

```
vectorPath=hdfs://my-hdfs/nodeToVector
queryPath=hdfs://my-hdfs/queryNodeToVertor
outputPath=hdfs://my-hdfs/output

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --conf spark.ps.instances=1 \
  --conf spark.ps.cores=1 \
  --conf spark.ps.jars=$SONA_ANGEL_JARS \
  --conf spark.ps.memory=10g \
  --name "swing angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.HnswExample \
  ../lib/spark-on-angel-examples-3.3.0.jar
  vectorPath:$vectorPath queryPath:$queryPath outputPath:$outputPath itemSep:colon vecSep:space saveItemSep:tab storageLevel:MEMORY_ONLY \
  partitionNum:4 psPartitionNum:1 distanceFunction:cosine-distance queryPartitionNum:4 ef:40 efConstruction:40 M:16 maxM:16 maxM0:32 mL:1.0
```

#### 常见问题

- 在差不多10min的时候，任务挂掉： 很可能的原因是angel申请不到资源！由于HNSW基于Spark On
  Angel开发，实际上涉及到Spark和Angel两个系统，在向Yarn申请资源时是独立进行的。
  在Spark任务拉起之后，由Spark向Yarn提交Angel的任务，如果不能在给定时间内申请到资源，就会报超时错误，任务挂掉！ 解决方案是：
  1）确认资源池有足够的资源 2） 添加spark conf: spark.hadoop.angel.am.appstate.timeout.ms=xxx 调大超时时间，默认值为600000，也就是10分钟

