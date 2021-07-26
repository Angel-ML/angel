# Node2Vec

> Node2Vec是一种知名的图表示学习算法。它能够通过结合深度优先搜索与广度优先搜索的优势采样对节点进行采样得到游走节点序列，该采样方式能够同时提取网络结构中homophily equivalence与structural equivalence两种相似特性。采样生成的序列输入Word2Vec学习出各节点的embedding，详情可参考论文[Node2Vec](https://dl.acm.org/doi/pdf/10.1145/2939672.2939754)。在算法实现上将游走节点序列采样和节点的embedding学习分开，该算法类只实现采样获得游走节点序列的步骤，节点的embedding学习则需要在该运行完成该算法后再运行Word2Vec算法完成。

## 1. 算法实现介绍

我们基于Spark On Angel框架实现了该算法，其能够处理大规模工业级数据。节点的邻居集表(无边权)或Alias表(带边权)存储于Angel PSs上，spark executors根据batch数据从PSs上拉取对应节点的数据进行采样计算最终获得各个节点的游走节点序列。

## 2. 运行

### 算法IO参数

  - input： HDFS路径，无边权或带边权的邻接表，以指定分隔符进行分隔，比如：
        0	1	0.3
        2	1	0.5
        3	1	0.1
        3	2	0.7
        4	1	0.3
	其中，从左到右三列数据的意义分别为源节点ID、尾节点ID、边权值(无权情况可以为空)。
  - output： HDFS路径, 节点的游走路径输出

### 算法参数

  - isWeighted： 是否带边权，是为true，否为false
  - delimiter： 分隔符，可选space、comma、tab
  - needReplicaEdge： 是否需要进行重复边生成，若输入邻接表存在边a->b却无b->a则需要(true)，否则不需要(false)
  - epochNum： 每个节点采样的游走路径条数
  - walkLength： 每条游走路径的长度(节点数)
  - useTrunc： 是否采用随机切边策略，是为true，否为false
  - truncLength： 随机切边策略中拉取节点邻居数的上限 
  - batchSize： 每个mini batch的大小，一般设置为500～2000
  - psPartitionNum：PS上的数据分区个数，最好是parameter server个数的整数倍，让每个ps承载的分区数量相等，让每个PS负载尽量均衡, 一般设置为(ps数*cores数)的2～3倍
  - dataPartitionNum：输入数据的分区数，一般设为(spark executor数*executor core数)的3-4倍
  - setCheckPoint： 是否对PS上存储的邻居集表或Alias表进行checkpoint，是为true，否为false
  - pValue： 广度优先控制参数（p>max{q,1}偏向于深度优先，p<max{q,1}偏向于广度优先）
  - qValue： 深度优先控制参数（q>1偏向于广度优先，q<1偏向于深度优先）

### 资源参数

  - Angel PS个数和内存大小：ps.instance与ps.memory的乘积是ps总的配置内存。为了保证Angel PSs正常运行，需要配置PS上所存储数据所占空间大小两倍左右的内存。对于无权与带权的Node2Vec来说，计算公式分别为： 总节点数 * (各节点平均邻居数 + 1) * 8 (Bytes)；总节点数 * ((各节点平均邻居数 + 1) * 8 + (各节点平均邻居数 * 2) * 4) (Bytes)。
  - Spark的资源配置：num-executors与executor-memory的乘积是executors总的配置内存，最好能存下2倍的输入数据。 如果内存紧张，1倍也是可以接受的，但是相对会慢一点。 在资源实在紧张的情况下， 尝试加大分区数目。
  
### 任务提交示例
进入angel环境bin目录下
```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/walkpaths_n2v

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
  --class com.tencent.angel.spark.examples.cluster.Node2VecExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output isWeighted:false delimiter:space needReplicaEdge:true epochNum:1 walkLength:20 useTrunc:false truncLength:6000 batchSize:1000 setCheckPoint:true pValue:0.8 qValue:1.2
```
