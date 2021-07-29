# EGES

> EGES(enhanced graph embedding with side information)是一种不仅利用图结构信息，并且利用节点的副信息（side information）共同学习出节点的embedding的图表示学习算法。具体来说，节点的副信息包括若干离散属性，EGES算法通过学习某个节点本身以及其对应各种离散属性的embedding，并同时自适应学习出各embedding的权值，最终将各embedding的加权平均结果作为该节点最终的embedding。相比只基于图结构的图表示学习算法，EGES能够获得更优表示学习性能。另外，对于新出现的节点，其与原图中的任意节点均不存在连边，即“cold start”问题。在这种情况下，EGES可以对该节点的各个离散属性的embedding取平均作为该节点的embedding，从而一定程度上缓解cold start问题。更多细节可参考论文[EGES](https://dl.acm.org/doi/abs/10.1145/3219819.3219869)。

## 1. 算法实现介绍

我们基于Spark on Angel框架实现了EGES算法，能够处理大规模工业级数据。所有节点与副信息的embedding以及对应权值存储在Angel PSs上。在每次epoch中，各个Spark executor根据每个batch数据从PS上拉取对应正样本节点、副信息的输入embedding与对应权值以及负样本节点的输出embedding并计算embedding与权值的梯度，再将梯度上推到PSs上对对应节点和副信息的embedding与权值进行更新。

## 2. 运行

### 算法IO参数

  - input： HDFS路径，每条数据为共现节点对以及源节点的各副信息，以空白符或者逗号分隔，比如：
        0	1	sideInfo1	sideInfo2
        2	1	sideInfo3	sideInfo4
        3	1	sideInfo5	sideInfo6
        3	2	sideInfo5	sideInfo6
        4	1	sideInfo7	sideInfo8
  - output： HDFS路径, 保存节点ID重编码映射表、边信息ID重编码映射表、节点最终的加权平均embedding(节点本身与边信息的embedding加权平均后的结果)。三者分别位于output路径下的itemMappingTab、sideInfoMappingTab、aggregateItemEmbedding三个子目录下。
  - matrixOutput：HDFS路径，保存节点与副信息本身的embedding矩阵、节点embedding的权值矩阵，二者分别位于matrixOutput路径下的embedding、weightsSI两个子目录下。
  - saveModelInterval：两次模型保存操作之间的epoch间隔数
  - checkpointInterval：两次checkpoint操作之间的epoch间隔数

### 算法参数

  - needRemapping： 是否需要对节点ID与副信息ID进行重编码，需要则设置为true，否则设置为false。编码规则为：节点ID从0开始进行连续编码，而副信息ID紧接着最大的节点重编码ID之后进行连续编码。
  - weightedSI： 是否需要对节点及其副信息的embedding权值进行自适应学习
  - numWeightsSI： 每个节点的副信息个数 
  - embeddingDim： embedding的维数
  - numNegSamples： 负样本数
  - numEpoch： epoch数
  - chepointInterval： 对PSs上的embedding矩阵与权值矩阵进行checkpoint，相邻两次checkpoint间的epoch间隔数
  - saveModelInterval： 对PSs上的embedding矩阵与权值矩阵进行保存，相邻两次保存操作的epoch间隔数
  - batchSize： 每个mini batch的大小
  - stepSize： 学习步长
  - decayRate： 学习步长衰减参数
  - dataPartitionNum：输入数据的分区数，一般设为(spark executor数*executor core数)的3-4倍
  - psPartitionNum：PS上的数据分区个数，最好是parameter server个数的整数倍，让每个ps承载的分区数量相等，让每个PS负载尽量均衡, 一般设置为(ps数*cores数)的2～3倍

### 资源参数

  - Angel PS个数和内存大小：ps.instance与ps.memory的乘积是ps总的配置内存。为了保证Angel PS稳定运行，需要配置约模型大小两倍的内存。对于EGES来说，模型大小的计算公式为： ( (节点与副信息总个数 * Embedding的维数 * 2 * 4) + (节点数 * （副信息数 + 1）* 4) ) （Bytes）。
  - Spark的资源配置：num-executors与executor-memory的乘积是executors总的配置内存，最好能存下2倍的输入数据。 如果内存紧张，1倍也是可以接受的，但是相对会慢一点。 在资源实在紧张的情况下， 尝试加大分区数目。

### 任务提交示例

进入angel环境bin目录下
```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/EGES_output
matrixOutput=hdfs://my-hdfs/EGES_matrixOutput

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --conf spark.ps.instances=1 \
  --conf spark.ps.cores=1 \
  --conf spark.ps.jars=$SONA_ANGEL_JARS \
  --conf spark.ps.memory=10g \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.EGESExample \
  ../lib/spark-on-angel-examples-3.1.0.jar \
  input:$input output:$output matrixOutput:$matrixOutput weightedSI:true numWeightsSI:3 embeddingDim:32 numNegSamples:5 epochNum:10 stepSize:0.01 decayRate:0.5 batchSize:1000 dataPartitionNum:12 psPartitionNum:10 needRemapping:false
```
