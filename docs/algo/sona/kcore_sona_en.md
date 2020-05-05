# K-CORE

> The K-CORE (k-degenerate-graph) algorithm is an important index in complex network research with 
a wide range of applications.

## 1. Algorithm Introduction
We have implemented k-core algorithm for large-scale networks based on Spark On Angel. According to the algorithm theory of K-Core, we can know that to calculate the coreness value of a node, we need to scan the coreness value of all its neighbors. If we store the coreness value of the node on the parameter server, in each iteration, we need to pull down the coreness of all neighbors of the node from the parameter server. In order to take advantage of the sparsity of the iteration (the coreness of most nodes will not change as the iteration progresses), we store the coreness of the node on the Spark Executor side, and only store the coreness value of the changed node on the parameter server side.

![kcore_structure](../../img/kcore_structure.png)

When calculating K-Core, it is necessary to store variable data corresponding to the edges in the graph (est[][], an array of coreness estimates of the neighbors of each node), we store it in the Executor together with the adjacency table . The specific implementation architecture of K-Core is shown in the figure above. The PS side stores two node vectors `reader` and `writer`, where `reader` corresponds to the coreness of the node updated in the previous iteration, and `writer` corresponds to the coreness of the node being updated in this iteration; the Executor side needs to store in addition to the adjacency list. The coreness corresponding to each node of the subgraph and its neighbors. The specific calculation process is as follows:

1. Executor pulls the coreness of the updateable node in est[][] from the `reader` to update it;
2. For each node in the Executor, calculate the h-index corresponding to est[] and update the estimated coreness of the node;
3. Push the coreness updated in step 2 to the `writer`;
4. After all Executors finish the calculation in steps 1-3, the `writer` on the PS side is the new `reader` for the next round, and the `writer` is reset at the same time.

## 2. Running

### IO Parameters

- input: hdfs path, input network data, two long integer id nodes per line, separated by white space or comma, indicating an edge
- output: hdfs path, output the coreness corresponding to the node, one data per line, indicating the coreness value corresponding to the node, separated by tap character
- sep: input data separator, support: space, comma, tab, default is space
- srcIndex: source node index, default is 0
- dstIndex: target node index, default is 1

### Algorithm Parameters

- batchSize: the size of the node update batch
- partitionNum: Enter the number of data partitions
- psPartitionNum: ps partition number
- useBalancePartition: whether to use balanced partition, the default is false
- balancePartitionPercent: Balance partition degree, the default is 0.7


### Task Submission Example
Enter the bin directory of the angel environment

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
  --class com.tencent.angel.spark.examples.cluster.KCoreExample \
  ../lib/spark-on-angel-examples-3.1.0.jar
  input:$input output:$output sep:" " batchSize:1000 partitionNum:2 psPartitionNum：2 useBalancePartition:false 
```