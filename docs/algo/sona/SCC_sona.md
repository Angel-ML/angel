# SCC

> SCC(connected components)算法用于求解图的强连通分量。

## 1. 算法介绍
SCC算法处理有向图中的强连通分量的计算问题。对在同一个强连通分量中的节点，分配一个相同的标签。我们基于Spark On Angel实现了大规模网络上的强连通分量算法。
算法中对图上节点分为两种状态，final节点指已确定其所属强连通分量标签的节点，非final节点为待确定节点。
#### 算法流程
1. 处理图中所有入度中非final节点或者出度中非final节点个数为0的节点，将其标记为final节点，其标签记录为当前值；
2. 将经过1处理的图沿着边的方向进行染色，路径上的节点颜色记录为路径上最小的节点id；
3. 考虑节点id与节点颜色标签相同的节点所在的强连通分量，将该节点设为final；
4. 那么如果一个节点存在边指向了一个相同颜色标签的final节点，那么该节点也属于该强连通分量，将其设为final，反复执行，直到无节点可被设为final；
5. 恢复所有非final节点在进行染色之前的标签值；
6. 循环执行1-5操作，直到所有节点都为final。

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
  --name "scc angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class org.apache.spark.angel.examples.graph.SCCExample \
  ../lib/spark-on-angel-examples-3.1.0.jar
  input:$input output:$output sep:tab storageLevel:MEMORY_ONLY useBalancePartition:true \
  partitionNum:4 psPartitionNum:1
```

#### 常见问题
- SCC效率与图结构有关，其算法中大量的循环处理会导致稀疏图上的效率偏低，处理大图的效率有待提高。