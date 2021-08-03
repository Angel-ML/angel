# weighted H-Index
## 1. Algorithm Introduction
weighted H-Index calculates the h-index value for each node in a undirected weighted graph, the detailed formula can be referred to page 51 of [Vital nodes identification in complex networks](https://www.sciencedirect.com/science/article/abs/pii/S0370157316301570). The h-index value is usually used to represent the importance of a vertex.

## 2. Parameters
#### IO Params

- input：hdfs path for a undirected and unweighted graph, each row represents an edge in the form of `srcId | dstId`
- output: hdfs path for output, each row represents the vertex and the corresponding hindex/gindex/windex value in the form of `nodeId | hindex | gindex | windex`
- isWeighted: whether the graph is weighted, if not, a default weighte value of 1.0 is set to each edge
- sep: the separation in input file to separate the srcId and dstId, could be tab, space or comma

#### Algo Params

- partitionNum：num of RDD partitions
- psPartitionNum：num of data partitions on ps
- storageLevel：RDD persist level，`DISK_ONLY`/`MEMORY_ONLY`/`MEMORY_AND_DISK`

## 3. Running

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
  --name "whindex angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class org.apache.spark.angel.examples.graph.WeightedHIndexExample \
  ../lib/spark-on-angel-examples-3.2.0.jar
  input:$input output:$output sep:tab storageLevel:MEMORY_ONLY isWeighted:true \
  partitionNum:4 psPartitionNum:1
```
