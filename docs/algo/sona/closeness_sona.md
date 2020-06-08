# Closeness

>Closeness算法，用于度量每个顶点在图中的中心程度。closeness作为节点重要性评估的核心指标之一，常用于关键点识别以及营销传播等场景。

## 1. 算法介绍

我们基于Spark On Angel和论文《Centralities in Large Networks: Algorithms and Observations》实现了大规模的Closeness计算。

## 2. 分布式实现

Closeness的实现过程中，采用HyperLogLog++基数计数器记录每个顶点n阶邻居数。采用类似HyperAnf的思路对Closeness进行近似计算。

## 3. 运行

### 算法IO参数
  - input： 输入，hdfs路径，无向图/有向图，不带权，每行表示一条边，srcId 分隔符 dstId，分隔符可以为空白符、tab或逗号等
  - output： 输出，hdfs路径，保存计算结果。输出为nodeId(long) | closeness(float) | 节点cardinality | 半径加权求和的cardinality, closeness值越大表示节点越重要
  - sep: 分隔符，输入中每条边的起始顶点、目标顶点之间的分隔符: `tab`, `空格`等
### 算法参数
  - partitionNum： 数据分区数，worker端spark rdd的数据分区数量，一般设为spark executor个数乘以executor core数的3-4倍，
  - psPartitionNum： 参数服务器上模型的分区数量，最好是parameter server个数的整数倍，让每个ps承载的分区数量相等，让每个PS负载尽量均衡, 数据量大的话推荐500以上
  - msgNumBatch： spark每个rdd分区分批计算的次数
  - useBalancePartition：是否使用均衡分区，默认为false
  - balancePartitionPercent：均衡分区度，默认为0.7
  - verboseSaving: 详细保存closeness中间结果
  - isDirected：是否为有向图，默认为true
  - storageLevel：RDD存储级别，`DISK_ONLY`/`MEMORY_ONLY`/`MEMORY_AND_DISK`

### 资源参数

- ps个数和内存大小：ps.instance与ps.memory的乘积是ps总的配置内存。为了保证Angel不挂掉，需要配置ps上数据存储量大小两倍左右的内存。对于Closeness算法来说，ps上放置的是各顶点的一阶邻居，数据类型是(Long，Array[Long]),据此可以估算不同规模的Graph输入下需要配置的ps内存大小
- Spark的资源配置：num-executors与executor-memory的乘积是executors总的配置内存，最好能存下2倍的输入数据。 如果内存紧张，1倍也是可以接受的，但是相对会慢一点。 比如说100亿的边集大概有600G大小， 50G * 20 的配置是足够的。 在资源实在紧张的情况下， 尝试加大分区数目！

### 任务提交示例

```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/output

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --conf spark.ps.instances=1 \
  --conf spark.ps.cores=1 \
  --conf spark.ps.jars=$SONA_ANGEL_JARS \
  --conf spark.ps.memory=10g \
  --name "commonfriends angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class org.apache.spark.angel.examples.cluster.ClosenessExample \
  ../lib/spark-on-angel-examples-3.1.0.jar
  input:$input output:$output sep:tab storageLevel:MEMORY_ONLY useBalancePartition:true \
  balancePartitionPercent:0.7 partitionNum:4 psPartitionNum:1 msgNumBatch:8 \   
  pullBatchSize:1000 verboseSaving:true src:1 dst:2 mode:yarn-cluster
```



### 常见问题
  - 在差不多10min的时候，任务挂掉： 很可能的原因是angel申请不到资源！由于Closeness基于Spark On Angel开发，实际上涉及到Spark和Angel两个系统，它们的向Yarn申请资源是独立进行的。 在Spark任务拉起之后，由Spark向Yarn提交Angel的任务，如果不能在给定时间内申请到资源，就会报超时错误，任务挂掉！ 解决方案是： 1）确认资源池有足够的资源 2） 添加spakr conf: spark.hadoop.angel.am.appstate.timeout.ms=xxx 调大超时时间，默认值为600000，也就是10分钟
  - 如何估算我需要配置多少Angel资源： 为了保证Angel不挂掉，需要配置模型大小两倍左右的内存 另外，在可能的情况下，ps数目越小，数据传输的量会越小，但是单个ps的压力会越大，需要一定的权衡。
  - Spark的资源配置： 同样主要考虑内存问题，最好能存下2倍的输入数据。 如果内存紧张，1倍也是可以接受的，但是相对会慢一点。 比如说100亿的边集大概有600G大小， 50G * 20 的配置是足够的。 在资源实在紧张的情况下， 尝试加大分区数目！
