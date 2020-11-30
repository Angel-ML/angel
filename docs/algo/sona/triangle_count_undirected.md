# TriangleCountUndirected
## 1. 算法介绍
TriangleCountUndirected是计算每个Graph中节点所在的三角形个数的算法。

## 2. 运行
#### 算法IO参数

- input：输入，hdfs路径，无向图，不带权。每行表示一条边： srcId 分隔符 dstId
- output: 输出，hdfs路径。每行表示一个顶点及其对应的三角形个数和局部聚集系数(如果computeLCC为true)：
  - 当computeLCC为true时， 输出为3列(tab间隔)：nodeId tab 三角形个数 tab 局部聚集系数
  - 当computeLCC为false时， 输出为2列(tab间隔)：nodeId tab 三角形个数
- sep: 分隔符，输入中每条边的起始顶点、目标顶点之间的分隔符: `tab`, `空格`等

#### 算法参数

- partitionNum：数据分区数，spark rdd数据的分区数量
- psPartitionNum：参数服务器上模型的分区数量
- batchSize: 向ps推送邻接表时的mini batch大小
- pullBatchSize: 计算顶点的三角形个数时的mini batch大小（每次计算pullBatchSize个顶点）
- computeLCC: 是否同时计算顶点的局部聚集系数
- storageLevel：RDD存储级别，`DISK_ONLY`/`MEMORY_ONLY`/`MEMORY_AND_DISK`

#### 资源参数

- ps个数和内存大小：ps.instance与ps.memory的乘积是ps总的配置内存。为了保证Angel不挂掉，需要配置ps上数据存储量大小两倍左右的内存。对于三角计数来说，ps上放置的是各顶点的一阶邻居，数据类型是(Long，Array[Long]),据此可以估算不同规模的Graph输入下需要配置的ps内存大小
- Spark的资源配置：num-executors与executor-memory的乘积是executors总的配置内存，最好能存下2倍的输入数据。 如果内存紧张，1倍也是可以接受的，但是相对会慢一点。 比如说100亿的边集大概有600G大小， 50G * 20 的配置是足够的。 在资源实在紧张的情况下， 尝试加大分区数目！

#### 任务提交示例

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
  --name "hindex angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class org.apache.spark.angel.examples.graph.TriangleCountUndirectedExample \
  ../lib/spark-on-angel-examples-3.1.0.jar
  input:$input output:$output sep:tab storageLevel:MEMORY_ONLY useBalancePartition:true \
  partitionNum:4 psPartitionNum:1 batchSize:3000 pullBatchSize:1000 computeLCC:false
```

#### 常见问题
- 在差不多10min的时候，任务挂掉： 很可能的原因是angel申请不到资源！由于该三角计数的实现是基于Spark On Angel开发，实际上涉及到Spark和Angel两个系统，在向Yarn申请资源时是独立进行的。 在Spark任务拉起之后，由Spark向Yarn提交Angel的任务，如果不能在给定时间内申请到资源，就会报超时错误，任务挂掉！ 解决方案是： 1）确认资源池有足够的资源 2） 添加spakr conf: spark.hadoop.angel.am.appstate.timeout.ms=xxx 调大超时时间，默认值为600000，也就是10分钟

