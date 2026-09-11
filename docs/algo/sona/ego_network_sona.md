# Ego Network

## 1. 算法介绍

> Ego Network是由一个中心节点（Ego）和与其直接连接的邻居节点（Alter）组成的网络，在社交网络分析中也称为Personal
> Network。该网络中的边展示了Ego与Alter和Alter与Alter之间的联系。Ego
> Network中的每个Alter也有属于它自己的Ego Network，每个Ego Network就形成了社交网络。Ego
> Network可应用于下列场景：（1）社会援助，如情感支持和物资支援等；（2）网络含义发现，比如如何解读世界等；
> （3）社会治理，如确保个人行为符合规范等；（4）资源发掘，如通过网络Ego节点获取用户或者招聘等。

## 2. 运行

#### 算法IO参数

- input：hdfs路径，输入边数据
- output：hdfs路径，结果输出路径
- nodePath：hdfs路径，指定需要提取ego network的节点数据路径，不配置则使用input的所有节点
- needReplicaEdges：读取输入边数据时，是否保留重复边
- sepInNodePath：输入节点数据列内部分隔符(space, comma, tab, colon), 默认为space
- sep：输入边数据列内部分隔符(space, comma, tab, colon), 默认为tab

#### 算法参数

- psPartitionNum：模型分区个数，最好是parameter server个数的整数倍，让每个ps承载的分区数量相等，让每个PS负载尽量均衡,
  数据量大的话推荐500以上
- partitionNum：输入RDD数据分区大小，一般设为spark executor个数乘以executor core数的3-4倍
- storageLevel：RDD存储级别，默认为MEMORY_ONLY
- batchSize：往ps推送邻接表信息时的batch大小
- pullBatchSize：从ps拉取邻接表信息时的batch大小
- version：值为`v1`或者`v2`。 v1版本输出节点的完整ego；v2版本适用于千亿边量级的图，只输出三角形和存在三角形的邻居。
- cpDir：RDD数据checkpoint hdfs地址

#### 资源参数

- Angel PS个数和内存大小：ps.instance与ps.memory的乘积是ps总的配置内存。为了保证Angel不挂掉，需要配置模型大小两倍左右的内存。
- Spark的资源配置：num-executors与executor-memory的乘积是executors总的配置内存，最好能存下2倍的输入数据。
  如果内存紧张，1倍也是可以接受的，但是相对会慢一点。 比如说100亿的边集大概有160G大小， 20G * 20 的配置是足够的。
  在资源实在紧张的情况下， 尝试加大分区数目！

#### 任务提交示例

```
input=hdfs://my-hdfs/path/of/edge
output=hdfs://my-hdfs/path/of/output
nodePath=hdfs://my-hdfs/path/of/node

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --conf spark.ps.instances=1 \
  --conf spark.ps.cores=1 \
  --conf spark.ps.jars=$SONA_ANGEL_JARS \
  --conf spark.ps.memory=10g \
  --name "swing angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class org.apache.spark.angel.examples.graph.SwingExample \
  ../lib/spark-on-angel-examples-3.3.0.jar
  input:$input output:$output nodePath:$nodePath sep:tab sepInNodePath:space needReplicaEdges:false \
  storageLevel:MEMORY_ONLY partitionNum:4 psPartitionNum:1 batchSize:10000 pullBatchSize:1000
```

#### 常见问题

- 在差不多10min的时候，任务挂掉： 很可能的原因是angel申请不到资源！由于Ego Network基于Spark On
  Angel开发，实际上涉及到Spark和Angel两个系统，在向Yarn申请资源时是独立进行的。
  在Spark任务拉起之后，由Spark向Yarn提交Angel的任务，如果不能在给定时间内申请到资源，就会报超时错误，任务挂掉！ 解决方案是：
  1）确认资源池有足够的资源 2） 添加spark conf: spark.hadoop.angel.am.appstate.timeout.ms=xxx 调大超时时间，默认值为600000，也就是10分钟

