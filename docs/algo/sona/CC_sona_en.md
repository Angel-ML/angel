# CC

> CC(connected components) algorithm is used to calculate the connected components of a graph.

## 1. Algorithm Introduction
CC algorithm treats every graph as the undirected. It assigns the same label to nodes belonging to the same connected component. We implemented cc algorithm for large-scale networks based on Spark On Angel.
The ps maintains the node's latest estimation of labels.
The Spark side maintains the adjacency list of the network, and pulls the latest label estimate in each round.
According to the cc algorithm, the label estimate of the node is updated, and then pushed back to the ps in each round.
The algorithm stops until none of labels of nodes are updated last round.
Every component is assigned with an unique id.
This implementation combines the advantages of distributed algorithm on big data with the advantages of union-find algorithm on a single machine.
The processing of connected components is divided into two parts: distributed part and union-find part on a single machine, and the results of the two parts are combined into the final result at the end.

## 2. Running

### Parameters
#### IO Parameters
- input： hdfs path of input data, one line for each edge, separate by blank, tab or a comma
- output： hdfs path of output, each line for a pair of node id with its label, separated by tap
- sep: data column separator (space, comma, tab), default is space.

#### Algorithm Parameters
- partitionNum： The number of input data partitions is generally set to 3-4 times the number of spark executors times the number of executor cores.
- psPartitionNum：The number of model partitions is preferably an integer multiple of the number of parameter servers, so that the number of partitions carried by each ps is equal, and the load of each PS is balanced as much as possible. If the amount of data is large, more than 500 is recommended.
- storageLevel：RDD storage level, `DISK_ONLY`/`MEMORY_ONLY`/`MEMORY_AND_DISK`
- localLimit: The upper limit of edges that can be processed by union-find algorithm on a single machine. If edges num is estimated to be smaller than localLimit, edges will be collected to local and do union-find.
- compressIterNum: Estimate the edges num of current graph every compressIterNum iterations.
- needReplicaEdge: Whether to construct undirected graphs with reversed edges.


#### Resource Parameters
- Angel PS number and memory: The product of ps.instance and ps.memory is the total configuration memory of ps. In order to ensure that Angel does not hang, you need to configure memory about twice the size of the model.
- Spark resource settings：The product of num-executors and executor-memory is the total configuration memory of executors, and it is best to store twice the input data. If the memory is tight, 1 times is also acceptable, but relatively slower. For example, a 10 billion edge set is about 160G in size, and a 20G * 20 configuration is sufficient. In a situation where resources are really tight, try to increase the number of partitions!

#### Submitting Scripts

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
  --name "cc angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class org.apache.spark.angel.examples.graph.CCExample \
  ../lib/spark-on-angel-examples-3.1.0.jar
  input:$input output:$output sep:tab storageLevel:MEMORY_ONLY useBalancePartition:true \
  partitionNum:4 psPartitionNum:1 localLimit:100000000 compressIterNum:3 needReplicaEdge:true
```

#### FAQ
-