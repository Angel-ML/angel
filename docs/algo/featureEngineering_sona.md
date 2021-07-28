# DataSampling
## 1. 算法介绍
该模块是一种常用的数据预处理方法，通常可作为其他算法的前提。它提供了从原数据集里随机抽取特定的比例或者特定数量的小样本的方法。
其他常见的算法模块可以通过配置抽样率完成数据抽样的功能，无需单独使用该模块；该模块常用于抽取小样本用于数据的可视化。<br>
说明：最终抽样的比例是min(抽样率， 抽样量/总数据量)。因此如果抽样量参数为1000，最终的抽样量不一定是精确的1000
## 2. 运行
#### 算法IO参数

- input：输入，任何数据
- output: 输出，抽样后的数据，格式与输入数据一致
- sep: 数据分隔符，支持：空格(space)，逗号(comma)，tab(\t)
- featureCols：表示需要计算的特征所在列，例如“1-10,12,15”，其说明取特征在表中的第1到第10列，第12列以及第15列，从0开始计数

#### 算法参数
- sampleRate：样本抽样率
- takeSample：抽样数目，选填
- partitionNum：数据分区数，spark rdd数据的分区数量

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
  --name "deepwalk angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.DeepWalkExample \
  ../lib/spark-on-angel-examples-3.2.0.jar
  input:$input output:$output \
  sep:tab storageLevel:MEMORY_ONLY useBalancePartition:true \
  partitionNum:4 psPartitionNum:1 walkLength:10 needReplicateEdge:true
```
