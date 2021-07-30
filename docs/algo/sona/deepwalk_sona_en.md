# DeepWalk 
## 1. Algorithm introduction
DeepWalk is a graph representation learning algorithm with depth-first traversal that repeatedly visits visited nodes. 
The DeepWalk algorithm uses a random walk (RandomWalk) method to sample nodes in the graph. Given the starting node of the current visit, 
randomly sample the node from its neighbors as the next visit node, and repeat this process until the visit sequence length meets the preset condition. 
This algorithm only includes the wandering part.
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

- walkLength: the walking length of each path (or the number of nodes)
- numWalks: the number of walking rounds, each round of walking refers to the length of walking walkLength starting from all types of nodes included in the metaPath
- needReplicateEdge: whether to convert graph to undirected graph
- partitionNum: the number of data partitions, the number of spark rdd data partitions
- psPartitionNum: the number of partitions of the model on the parameter server
- useBalancePartition: Whether the parameter server divides the storage partition of the input data node into a balanced partition, if the index of the input node is not uniform, it is recommended to choose yes
- storageLevel: RDD storage level, `DISK_ONLY`/`MEMORY_ONLY`/`MEMORY_AND_DISK`

#### Resource parameters

- ps number and memory size: the product of ps.instance and ps.memory is the total configured memory of ps. In order to ensure that Angel does not hang up, you need to configure memory that is about twice the size of the data storage on ps.
For deepwalk, the calculation formulas are: Number of nodes * (8+4+4)Bytes* Average number of neighbors of the node.
- Spark resource configuration: The product of num-executors and executor-memory is the total configuration memory of executors, and it is best to store 2 times the input data.
If the memory is tight, 1x is acceptable, but it will be relatively slow. For example, a 10 billion edge set is about 600G in size, and a 50G * 20 configuration is sufficient. When resources are really tight, try to increase the number of partitions!

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

#### FAQ
  - At about 10 minutes, the task hangs: The most likely reason is that Angel cannot apply for resources! Since DeepWalk is developed based on Spark On Angel, it actually involves two systems, Spark and Angel, and their application for resources from Yarn is carried out independently. After the Spark task is started, Spark submits the Angel task to Yarn. If the resource cannot be applied for within a given time, a timeout error will be reported and the task will hang! The solution is: 1) Confirm that the resource pool has sufficient resources 2) Add spark conf: spark.hadoop.angel.am.appstate.timeout.ms = xxx to increase the timeout time, the default value is 600000, which is 10 minutes
