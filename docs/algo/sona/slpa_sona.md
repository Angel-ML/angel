# SLPA算法
## 1. 算法介绍
SLPA（Speaker-listener Label Propagation Algorithm）由Jierui Xie等人于2011年提出，是一种重叠型社区发现算法，其中涉及一个重要阈值参数r，通过r的适当选取，可将其退化为非重叠型。
它是对LPA算法（标签传播算法）的拓展。SLAP重叠社区的发现主要认为每个节点不光只有一个标签，每次迭代增加一个标签，最后再通过某种策略进行筛选。

## 2. 运行
#### 算法IO参数

- input：输入，hdfs路径，无向或者有向图，有权或者无权（可选）。每行表示一条边： srcId 分隔符 dstId （分隔符 weight）。比如： 
0 1 0.3 
2 1 0.5 
3 1 0.1 
3 2 0.7 
4 1 0.3 
其中，从左到右三列数据的意义分别为源节点ID、尾节点ID、边权值(无权情况可以为空)
- output: 输出，hdfs路径。每行表示一条游走路径
- sep: 分隔符，输入中每条边的起始顶点、目标顶点之间的分隔符: `tab`, `空格`等
- isWeighted：边是否带权


#### 算法参数

- maxIteration：最大迭代次数
- numMaxCommunities：表示每个顶点最多同时处于numMaxCommunities个社区中
- preserveRate：每次计算顶点所属社区时，会有一定概率保留上次结果，即该顶点该次迭代不做更新
- needReplicateEdge：是否需要将graph转成无向图
- partitionNum：数据分区数，spark rdd数据的分区数量
- psPartitionNum：参数服务器上模型的分区数量
- useBalancePartition：参数服务器对输入数据节点存储划分是否均衡分区，如果输入节点的索引不是均匀的话建议选择是
- storageLevel：RDD存储级别，`DISK_ONLY`/`MEMORY_ONLY`/`MEMORY_AND_DISK`

#### 资源参数

- ps个数和内存大小：ps.instance与ps.memory的乘积是ps总的配置内存。为了保证Angel不挂掉，需要配置ps上数据存储量大小两倍左右的内存。对于SLPA算法来说，如果是1亿节点，numMaxCommunities为3的话，模型大小为 1亿*(8+4)bytes*3*2 大小为7.2G，那么配置instances=2, memory=8G 就差不多了
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
  --name "slpa angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.SLPAExample \
  ../lib/spark-on-angel-examples-3.2.0.jar
  input:$input output:$output \
  sep:tab storageLevel:MEMORY_ONLY useBalancePartition:true maxIteration：10\
  partitionNum:4 psPartitionNum:1 numMaxCommunities:3 needReplicateEdge:true
```

#### 常见问题
- 在差不多10min的时候，任务挂掉： 很可能的原因是angel申请不到资源！由于DeepWalk基于Spark On Angel开发，实际上涉及到Spark和Angel两个系统，在向Yarn申请资源时是独立进行的。 在Spark任务拉起之后，由Spark向Yarn提交Angel的任务，如果不能在给定时间内申请到资源，就会报超时错误，任务挂掉！ 解决方案是： 1）确认资源池有足够的资源 2） 添加spakr conf: spark.hadoop.angel.am.appstate.timeout.ms=xxx 调大超时时间，默认值为600000，也就是10分钟

