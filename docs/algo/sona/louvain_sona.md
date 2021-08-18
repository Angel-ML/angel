# Louvain(FastUnfolding)

> Louvain(FastUnfolding)算法是经典的社区发现算法, 通过优化[模块度](https://en.wikipedia.org/wiki/Modularity)指标来达到社区划分的目的。

## 1. 算法介绍
Louvain算法包含两个过程
 - 模块度优化
 - 社区折叠
我们通过两个ps向量来维护节点的社区id以及社区id对应的权重信息。Spark端每个worker维护一部分节点和对应的邻接信息，包括节点的邻居以及对应的连边权重。
- 在模块度优化阶段，每个worker根据模块度变化量计算自己维护节点的新的社区归属。社区归属的更新以batch的形式实时更新到ps。
- 在社区折叠阶段，我们根据当前的社区归属情况，构造新的网络，其中新的网络节点对应与折叠前网络的社区，新的连边对应于折叠前网络社团直接节点的连边权重之和。在开始下一阶段的模块度优化之前，我们需要校正社区id，使得每个社区的id标识为社区内某个节点的id。 这里我们使用社区中所有节点id最小的作为标识。

## 2. 运行

### 算法IO参数

- input： hdfs路径，输入网络数据，每行两个长整形id表示的节点（如果是带权网络，第三个float表示权重），以空白符或者逗号分隔，表示一条边
- output： hdfs路径， 输出节点对应的社区归属， 每行一条数据，表示节点对应的社区id值，以tap符分割
- sep: 输入数据分隔符，支持：空格，逗号，tab，默认为空格
- isWeighted：是否带权
- srcIndex： 源节点索引，默认为0
- dstIndex： 目标节点索引，默认为1
- weightIndex： 权重索引，默认为2

  
### 算法参数
- numFold： 折叠次数
- numOpt：每轮模块度优化次数
- eps：模块度增量下限
- batchSize：节点更新batch的大小
- partitionNum： 输入数据分区数
- psPartitionNum： ps分区数
- enableCheck：是否对社区id或度进行检查
- bufferSize： 缓冲区大小
- storageLevel： 存储级别
### 资源参数

  - Angel PS个数和内存大小：ps.instance与ps.memory的乘积是ps总的配置内存。为了保证Angel不挂掉，需要配置模型大小两倍左右的内存。 Louvain模型大小的计算公式为： 节点数*（8+8+4）Byte，比如说1亿节点，模型大小差不多有2G大小，那么配置instances=2, memory=2就差不多了。 另外，在可能的情况下，ps数目越小，数据传输的量会越小，但是单个ps的压力会越大，需要一定的权衡。
  - Spark的资源配置：num-executors与executor-memory的乘积是executors总的配置内存，最好能存下2倍的输入数据。 如果内存紧张，1倍也是可以接受的，但是相对会慢一点。 比如说100亿的边集大概有600G大小， 50G * 20 的配置是足够的。 在资源实在紧张的情况下， 尝试加大分区数目！

### 任务提交示例
进入angel环境bin目录下
```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/model

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --conf spark.ps.instances=1 \
  --conf spark.ps.cores=1 \
  --conf spark.ps.jars=$SONA_ANGEL_JARS \
  --conf spark.ps.memory=10g \
  --name "kcore angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.LouvainExample \
  ../lib/spark-on-angel-examples-3.2.0.jar
  input:$input output:$output numFold:10 numOpt:3 eps:0.0 batchSize:1000 partitionNum:2 psPartitionNum:2 enableCheck:false bufferSize:1000000 storageLevel:MEMORY_ONLY
```

#### 常见问题
- 在差不多10min的时候，任务挂掉： 很可能的原因是angel申请不到资源！由于Louvain基于Spark On Angel开发，实际上涉及到Spark和Angel两个系统，在向Yarn申请资源时是独立进行的。 在Spark任务拉起之后，由Spark向Yarn提交Angel的任务，如果不能在给定时间内申请到资源，就会报超时错误，任务挂掉！ 解决方案是： 1）确认资源池有足够的资源 2） 添加spakr conf: spark.hadoop.angel.am.appstate.timeout.ms=xxx 调大超时时间，默认值为600000，也就是10分钟
