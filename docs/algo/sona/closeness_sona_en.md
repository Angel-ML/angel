# Closeness

>The Closeness Centrality of a node measures its average farness(inverse diastance) to all other nodes.This algorithm aim to detecting nodes that are able to spread information very efficiently through a graph。

## 1. Algorithm Introduction

Based on spark on angel and the paper "Centralities in Large Networks: Algorithms and Observations" , we we implemented a large-scale closeness algorithm。

## 2. Distributed implementation

In the implementation of closeness, hyperloglog + + cardinal counter is used to record the n-order neighbors of each vertex. Similar to the idea of hyperanf algorithm, the approximate calculation of closeness is carried out.

## 3. Running Example

### Algorithm IO parameters

  - input： hdfs path for a undirected and unweighted graph, each row represents an edge in the form of `srcId | dstId
  - output： hdfs path for output, each row represents the vertex and its closeness value，include nodeId(long) | closeness(float) | node cardinality | adius weighted sum , the larger the closeness value, the more important the node is
  - sep:  the separation in input file to separate the srcId and dstId, could be tab, space or comma
### Algorithm parameters

  - partitionNum： The number of input data partitions is generally set to 3-4 times the number of spark executors times the number of executor cores
  - psPartitionNum：  The number of model partitions is preferably an integer multiple of the number of parameter servers, so that the number of partitions carried by each ps is equal, and the load of each PS is balanced as much as possible. If the amount of data is large, more than 500 is recommended
  - msgNumBatch： Number of batch calculations per RDD partition of spark
  - useBalancePartition：whether to use balancePartition strategy, `true` / `false`, `true` is suggested when the distribution of graph vertices is unbalanced
  - balancePartitionPercent：use balancePartition strategy 
  - verboseSaving: Save closeness intermediate results in detail
  - isDirected: whether directed graph
  - storageLevel：RDD storage level，`DISK_ONLY`/`MEMORY_ONLY`/`MEMORY_AND_DISK`

### Resource parameters

- Angel PS number and memory: The product of ps.instance and ps.memory is the total configuration memory of ps. In order to ensure that Angel does not hang, you need to configure memory about twice the size of the model. For PageRank, the calculation formula of the model size is: number of nodes * 3 * 4 Byte, according to which you can estimate the size of ps memory that needs to be configured under Graph input of different sizes
- Spark resource ：The product of num-executors and executor-memory is the total configuration memory of executors, and it is best to store twice the input data. If the memory is tight, 1 times is also acceptable, but relatively slower. For example, a 10 billion edge set is about 600G in size, and a 50G * 20 configuration is sufficient. In a situation where resources are really tight, try to increase the number of partitions!

### Submitting scripts

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
  --name "commonfriends angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class org.apache.spark.angel.examples.cluster.ClosenessExample \
  ../lib/spark-on-angel-examples-3.1.0.jar
  input:$input output:$output sep:tab storageLevel:MEMORY_ONLY useBalancePartition:true \
  balancePartitionPercent:0.7 partitionNum:4 psPartitionNum:1 msgNumBatch:8 \   
  pullBatchSize:1000 verboseSaving:true src:1 dst:2 mode:yarn-cluster
```



### FAQ

- At about 10 minutes, the task hangs: The most likely reason is that Angel cannot apply for resources! Since Common Friends is developed based on Spark On Angel, it actually involves two systems, Spark and Angel, and their application for resources from Yarn is independently conducted. After the Spark task is started, Spark submits the Angel task to Yarn. If the resource cannot be applied for within a given time, a timeout error will be reported and the task will hang! The solution is: 1) Confirm that the resource pool has sufficient resources 2) Add spakr conf: spark.hadoop.angel.am.appstate.timeout.ms = xxx to increase the timeout time, the default value is 600000, which is 10 minutes
- How to estimate how many Angel resources I need to configure: To ensure that Angel does not hang, you need to configure about twice the size of the model memory. In addition, when possible, the smaller the number of ps, the smaller the amount of data transmission, but the pressure of a single ps will be greater, requiring certain trade-offs.
- Spark resource allocation: Also mainly consider the memory problem, it is best to save twice the input data. If the memory is tight, 1 times is also acceptable, but relatively slower. For example, a 10 billion edge set is about 600G in size, and a 50G * 20 configuration is sufficient. In a situation where resources are really tight, try to increase the number of partitions!