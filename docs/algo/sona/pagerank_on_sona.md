# PageRank
>PageRank算法可能是最著名的节点重要性评价算法，最初由拉里佩奇提出，被应用于Google搜索的网页排名, 可参考论文[The PageRank Citation Ranking:Bringing Order to the Web](http://ilpubs.stanford.edu:8090/422/1/1999-66.pdf).

## 1. 算法介绍
我们基于Spark On Angel实现了大规模的PageRank计算，其中ps维护所有节点的的信息，包括接收、发送消息以及rank值向量。消息和rank值的计算在spark executor端完成，通过ps的push/update操作完成更新。

## 2. 运行

### 算法io参数

  - input: hdfs路径，每行表示一条边，可带权: srcId 分隔符 dstId 分隔符 weight(可选的)
  - output: hdfs路径，运算结果输出路径
  - sep: 数据列分隔符(space, comma, tab), 默认为space

### 算法参数

  - psPartitionNum：模型分区个数，最好是parameter server个数的整数倍，让每个ps承载的分区数量相等，让每个PS负载尽量均衡, 数据量大的话推荐500以上
  - dataPartitionNum：输入数据的partition数，一般设为spark executor个数乘以executor core数的3-4倍
  - tol：停止更新条件，越小表示结果越准确，默认为0.01
  - resetProp：随机重置概率(alpha), 默认为0.15

### 资源参数

  - Angel PS个数和内存大小：ps.instance与ps.memory的乘积是ps总的配置内存。为了保证Angel不挂掉，需要配置模型大小两倍左右的内存。对于PageRank来说，模型大小的计算公式为： 节点数 * 3 * 4 Byte，据此可以估算不同规模的Graph输入下需要配置的ps内存大小
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
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.PageRankExample \
  ../lib/spark-on-angel-examples-3.1.0.jar \
  input:$input output:$output tol:0.01 :5 resetProp:0.15
```

### 常见问题
  - 在差不多10min的时候，任务挂掉： 很可能的原因是angel申请不到资源！由于PageRank基于Spark On Angel开发，实际上涉及到Spark和Angel两个系统，它们的向Yarn申请资源是独立进行的。 在Spark任务拉起之后，由Spark向Yarn提交Angel的任务，如果不能在给定时间内申请到资源，就会报超时错误，任务挂掉！ 解决方案是： 1）确认资源池有足够的资源 2） 添加spakr conf: spark.hadoop.angel.am.appstate.timeout.ms=xxx 调大超时时间，默认值为600000，也就是10分钟
  - 如何估算我需要配置多少Angel资源： 为了保证Angel不挂掉，需要配置模型大小两倍左右的内存。另外，在可能的情况下，ps数目越小，数据传输的量会越小，但是单个ps的压力会越大，需要一定的权衡。
  - Spark的资源配置： 同样主要考虑内存问题，最好能存下2倍的输入数据。 如果内存紧张，1倍也是可以接受的，但是相对会慢一点。 比如说100亿的边集大概有600G大小， 50G * 20 的配置是足够的。 在资源实在紧张的情况下， 尝试加大分区数目！

