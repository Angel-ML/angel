# CommonFriends

> Common friends algorithm, aims to mine the number of common friends of two users; as a common graph feature / indicator, it is often used to describe the relationship between users, and widely used in friend recommendation, community detection, acquaintance / Stranger analysis and other scenarios。

## 1. Algorithm Introduction

Common friends this algorithm can be used in two scenarios:

1、 Input a total number of relationship chains, calculate the number of common friends with existing links, and can be used to describe the degree of relationship closeness.

2、 Input the total relationship chain and the edge table of the common friends to be calculated, and calculate the number of the common friends of the specified connection, which can be used for connection prediction or reasoning.

## 2. Distributed implementation

During the implementation of common friends, the adjacency table of vertices needs to be stored on multiple Parameter Servers，the calculation logic of common friends occurs in the worker, and it is necessary to pull the adjacency table of two vertices from Parameter Server to calculate the intersection, and obtain the number of common friends.

## 3. Running Example

### Algorithm IO parameters

- input： hdfs path for a undirected and unweighted graph, each row represents an edge in the form of `srcId | dstId`
- extraInput: hdfs path for a undirected and unweighted graph，an the data format is required to be the same as input. When extrainput and input paths are consistent, it is the first use scenario of the algorithm; When the paths of extrainput and input are different, the purpose is to calculate the number of common friends for a given edges, that is, the second use scenario of the algorithm.。
- output： hdfs path for output, each row represents the edge and the  common friends number of two vertices。
- sep: the separation in input file to separate the srcId and dstId, could be tab, space or comma

### Algorithm parameters

- partitionNum：The number of input data partitions is generally set to 3-4 times the number of spark executors times the number of executor cores.
- psPartitionNum： The number of model partitions is preferably an integer multiple of the number of parameter servers, so that the number of partitions carried by each ps is equal, and the load of each PS is balanced as much as possible. If the amount of data is large, more than 500 is recommended.
- batchSize： the mini batchSize of vertices when push neighborTable to ps
- pullBatchSize： the mini batchSize of vertices when calculating commo friends for each vertex
- isCompressed：Whether the edge is compressed or not. 1 indicates the compressed edge
- storageLevel：RDD storage level，`DISK_ONLY`/`MEMORY_ONLY`/`MEMORY_AND_DISK`

### Resource parameters

- Angel PS number and memory: The product of ps.instance and ps.memory is the total configuration memory of ps. In order to ensure that Angel does not hang, you need to configure memory about twice the size of the model. For PageRank, the calculation formula of the model size is: number of nodes * 3 * 4 Byte, according to which you can estimate the size of ps memory that needs to be configured under Graph input of different sizes
- Spark resource ：The product of num-executors and executor-memory is the total configuration memory of executors, and it is best to store twice the input data. If the memory is tight, 1 times is also acceptable, but relatively slower. For example, a 10 billion edge set is about 600G in size, and a 50G * 20 configuration is sufficient. In a situation where resources are really tight, try to increase the number of partitions!

### Submitting scripts

```
input=hdfs://my-hdfs/data
extraInput=hdfs://my-hdfs/data
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
  --class org.apache.spark.angel.examples.cluster.CommonFriendsExample \
  ../lib/spark-on-angel-examples-3.1.0.jar
  input:$input extraInput:$extraInput output:$output sep:tab storageLevel:MEMORY_ONLY useBalancePartition:true \
  partitionNum:4 psPartitionNum:1 batchSize:3000 pullBatchSize:1000 src:1 dst:2 mode:yarn-cluster
```



### FAQ

- At about 10 minutes, the task hangs: The most likely reason is that Angel cannot apply for resources! Since Common Friends is developed based on Spark On Angel, it actually involves two systems, Spark and Angel, and their application for resources from Yarn is independently conducted. After the Spark task is started, Spark submits the Angel task to Yarn. If the resource cannot be applied for within a given time, a timeout error will be reported and the task will hang! The solution is: 1) Confirm that the resource pool has sufficient resources 2) Add spakr conf: spark.hadoop.angel.am.appstate.timeout.ms = xxx to increase the timeout time, the default value is 600000, which is 10 minutes
- How to estimate how many Angel resources I need to configure: To ensure that Angel does not hang, you need to configure about twice the size of the model memory. In addition, when possible, the smaller the number of ps, the smaller the amount of data transmission, but the pressure of a single ps will be greater, requiring certain trade-offs.
- Spark resource allocation: Also mainly consider the memory problem, it is best to save twice the input data. If the memory is tight, 1 times is also acceptable, but relatively slower. For example, a 10 billion edge set is about 600G in size, and a 50G * 20 configuration is sufficient. In a situation where resources are really tight, try to increase the number of partitions!