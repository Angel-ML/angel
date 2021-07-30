# COPRA algorithm
## 1. Algorithm Introduction
The COPRA algorithm is an extended algorithm based on LPA, which can be used for the discovery of overlapping communities.

## 2. Running example
#### Algorithm IO parameters

- input：Input, hdfs path, undirected or directed graph, weight or no weight (optional). Each line represents an edge: srcId separator dstId (separator weight). for example：  <br>
0 1 0.3 <br>
2 1 0.5 <br>
3 1 0.1 <br>
3 2 0.7 <br>
4 1 0.3 <br>
Among them, the meanings of the three columns of data from left to right are the source node ID, the end node ID, and the edge weight (it can be empty if there is no weight)
- output: Output, hdfs path. Each row represents a wandering path
- sep: Separator, the separator between the start vertex and target vertex of each edge in the input: `tab`, `space`, etc.
- isWeighted：Whether the edge is weighted


#### Algorithm parameters

- maxIteration: the maximum number of iterations
- numMaxCommunities: Indicates that each vertex is in at most numMaxCommunities communities at the same time
- preserveRate: Each time the community to which a vertex belongs is calculated, there is a certain probability that the last result will be retained, that is, the vertex will not be updated in this iteration
- needReplicateEdge: whether you need to convert the graph to an undirected graph
- partitionNum: the number of data partitions, the number of spark rdd data partitions
- psPartitionNum: the number of partitions of the model on the parameter server
- useBalancePartition: Whether the parameter server divides the storage partition of the input data node into a balanced partition, if the index of the input node is not uniform, it is recommended to choose yes
- storageLevel: RDD storage level, DISK_ONLY/MEMORY_ONLY/MEMORY_AND_DISK

#### Resource parameters

- ps number and memory size: the product of ps.instance and ps.memory is the total configured memory of ps. 
In order to ensure that Angel does not hang up, you need to configure memory that is about twice the size of the data storage on ps. 
For the COPRA algorithm, if it is 100 million nodes and numMaxCommunities is 3, the model size is 100 million*(8+4)bytes*3*2 and the size is 7.2G, 
then the configuration of instances=2, memory=8G is almost the same
- Spark resource configuration: The product of num-executors and executor-memory is the total configuration memory of executors, 
and it is best to store 2 times the input data. If the memory is tight, 1x is acceptable, but it will be relatively slow. 
For example, a 10 billion edge set is about 600G in size, and a 50G * 20 configuration is sufficient. When resources are really tight, 
try to increase the number of partitions!
#### Submitting scripts

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
  --name "CopraExample angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.CopraExample \
  ../lib/spark-on-angel-examples-3.2.0.jar
  input:$input output:$output \
  sep:tab storageLevel:MEMORY_ONLY useBalancePartition:true maxIteration：10\
  partitionNum:4 psPartitionNum:1 numMaxCommunities:3 needReplicateEdge:true
```

#### FAQ
  - At about 10 minutes, the task hangs: The most likely reason is that Angel cannot apply for resources! Since COPRA is developed based on Spark On Angel, it actually involves two systems, Spark and Angel, and their application for resources from Yarn is carried out independently. After the Spark task is started, Spark submits the Angel task to Yarn. If the resource cannot be applied for within a given time, a timeout error will be reported and the task will hang! The solution is: 1) Confirm that the resource pool has sufficient resources 2) Add spark conf: spark.hadoop.angel.am.appstate.timeout.ms = xxx to increase the timeout time, the default value is 600000, which is 10 minutes
