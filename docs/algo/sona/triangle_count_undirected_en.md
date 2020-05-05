# TriangleCountUndirected
## 1. Algorithm Introduction
TriangleCounting is used to determine the number of triangles passing through each node in a undirected graph. Normally the Local Cluster Cofficient (LCC) is also calculated during the whole process. The returned result contains two or three columns, which represents the vertexID, num of triangles and the LCC value (optional).


## 2. Parameters
#### IO Params

- input：hdfs path for a undirected and unweighted graph, each row represents an edge in the form of `srcId | dstId`
- output: hdfs path for output, each row represents the vertex and the corresponding number of triangles and the lcc value(optional) in the form of `nodeId | numTriangles | lccValue(optional)`
- sep: the separation in input file to separate the srcId and dstId, could be tab, space or comma

#### Algo Params

- partitionNum：num of RDD partitions
- psPartitionNum：num of data partitions on ps
- batchSize: the mini batchSize of vertices when push neighborTable to ps
- pullBatchSize: the mini batchSize of vertices when calculating triangles for each vertex
- computeLCC: whether to compute the lcc value at the same time
- storageLevel：RDD persist level，`DISK_ONLY`/`MEMORY_ONLY`/`MEMORY_AND_DISK`

## Running

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
  --name "hindex angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class org.apache.spark.angel.examples.graph.TriangleCountUndirectedExample \
  ../lib/spark-on-angel-examples-3.1.0.jar
  input:$input output:$output sep:tab storageLevel:MEMORY_ONLY useBalancePartition:true \
  partitionNum:4 psPartitionNum:1 batchSize:3000 pullBatchSize:1000 computeLCC:false
```