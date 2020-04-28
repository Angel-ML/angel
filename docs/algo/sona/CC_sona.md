# CC

> CC(connected components)算法用于求解图的弱连通分量。

## 1. 算法介绍
CC算法将图看作无向图，对在同一个连通分量中的节点，分配一个相同的标签。我们基于Spark On Angel实现了大规模网络上的弱连通分量算法。
其中ps维护节点的最新的节点的标签值，Spark端维护网络的邻接表。每轮根据节点邻居标签对节点进行更新。直到无节点需要更新标签时终止。
算法将连通分量中的最小节点id作为该连通分量的标签。

## 2. 运行

### 参数
#### IO参数
- input： hdfs路径，输入网络数据，每行两个长整形id表示的节点，以空白符或者逗号分隔，表示一条边
- output： hdfs路径， 输出节点对应的标签值， 每行一条数据，表示节点对应的标签值，以tap符分割
- sep: 分隔符，输入中每条边的起始顶点、目标顶点之间的分隔符: `tab`, `空格`等

#### 算法参数
- partitionNum： 输入数据分区数
- psPartitionNum：参数服务器上模型的分区数量
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
  --name "cc angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class org.apache.spark.angel.examples.graph.CCExample \
  ../lib/spark-on-angel-examples-3.1.0.jar
  input:$input output:$output sep:tab storageLevel:MEMORY_ONLY useBalancePartition:true \
  partitionNum:4 psPartitionNum:1
```

#### 常见问题
-