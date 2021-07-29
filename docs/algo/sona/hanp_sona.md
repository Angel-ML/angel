# HANP

> HANP(Hop Attenuation & Node Preference)是一种基于标签传播的社区发现算法，详情可参考论文[HANP](https://arxiv.org/pdf/0808.2633.pdf）)。

## 1. 算法介绍

我们基于Spark on Angel框架实现了HANP算法，其能够处理大规模工业级数据。节点的度、标签、标签分数存储于Angel PSs上，每个Spark executor根据各自分区中的节点从PSs上拉取对应节点的数据进行计算，得到节点新的标签与标签分数，并将新的计算结果上推到PSs对对应节点的标签和标签分数进行更新。算法如此迭代进行拉取、计算与更新步骤，直到达到最大迭代次数结束。

## 2. 运行

### 算法IO参数

  - input： HDFS路径，无边权或带边权的邻接表，以指定分隔符进行分隔，比如：
        0	1	0.3
        2	1	0.5
        3	1	0.1
        3	2	0.7
        4	1	0.3
  - output： HDFS路径, 节点ID及其标签

### 算法参数

  - isWeighted： 是否带边权，是为true，否为false
  - sep： 数据分隔符，可选space、comma、tab
  - psPartitionNum： 模型分区个数，最好是parameter server个数的整数倍，让每个ps承载的分区数量相等，让每个PS负载尽量均衡, 数据量大的话推荐500以上
  - partitionNum： 输入数据的partition数，一般设为spark executor个数乘以executor core数的3-4倍
  - maxIteration： 最大迭代次数
  - preserveRate： 节点信息不更新的概率
  - delta： 每次标签传播时节点标签分数(score)的衰减量
  
### 资源参数

  - Angel PS个数和内存大小：ps.instance与ps.memory的乘积是ps总的配置内存。为了保证Angel PSs正常运行，需要配置PS上所存储数据所占空间大小两倍左右的内存。对于Hanp来说，计算公式为：(节点数 * (4 * 2 + 8 * 3)) (Bytes)。
  - Spark的资源配置：num-executors与executor-memory的乘积是executors总的配置内存，最好能存下2倍的输入数据。 如果内存紧张，1倍也是可以接受的，但是相对会慢一点。 在资源实在紧张的情况下， 尝试加大分区数目。

### 任务提交示例
进入angel环境bin目录下
```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/hanp_result

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
  --class com.tencent.angel.spark.examples.cluster.HanpExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output isWeighted:true sep:tab maxIteration:10 preserveRate:0.1 delta:0.1 psPartitionNum:10 partitionNum:12
```
