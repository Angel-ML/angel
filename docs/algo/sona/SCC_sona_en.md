# SCC

> SCC(strongly connected components) algorithm is used to calculate the strongly connected components of a graph.

## 1. Algorithm Introduction
On a directed graph, SCC algorithm assigns the same label to nodes belonging to the same strongly connected component. We implemented scc algorithm for large-scale networks based on Spark On Angel.
The ps maintains the node's latest estimation of label and status.
The Spark side maintains the adjacency list of the network, and pulls the latest label and status estimate in each round.
Each node in graph has two states: final or non-final. The final nodes are those whose labels is certain, the non-final uncertain.
#### Algorithm procedure
1. mark the nodes with zero in-degree or zero out-degree of the non-final neighbors as final nodes, their labels is what they wear now;
2. paint the non-final nodes along the edge direction, the color is the min id of nodes along the path;
3. consider the scc containing the node whose color is the same as its id, mark it as final.
4. if a node has edge refers to a non-final nodes with the same color, this node also belongs that strongly connected components, mark it as final. repeat until no nodes can change its state into final;
5. recover the non-final nodes' labels according to their labels before the painting process.
6. repeat process 1-5, until all nodes turn final.

The algorithm takes the min id of the node inside the connected component as the label of that component.

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

#### Resource Parameters
- Angel PS number and memory: The product of ps.instance and ps.memory is the total configuration memory of ps. In order to ensure that Angel does not hang, you need to configure memory about twice the size of the model.
- Spark resource settings：The product of num-executors and executor-memory is the total configuration memory of executors, and it is best to store twice the input data. If the memory is tight, 1 times is also acceptable, but relatively slower. For example, a 10 billion edge set is about 600G in size, and a 50G * 20 configuration is sufficient. In a situation where resources are really tight, try to increase the number of partitions!

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
  --class org.apache.spark.angel.examples.graph.SCCExample \
  ../lib/spark-on-angel-examples-3.1.0.jar
  input:$input output:$output sep:tab storageLevel:MEMORY_ONLY useBalancePartition:true \
  partitionNum:4 psPartitionNum:1
```

#### FAQ
- The efficiency of the scc algorithm is associated with the graph structure. The massive circulate process can cause low efficiency when processing sparse graph.