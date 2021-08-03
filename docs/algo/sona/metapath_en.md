# MetaPath Walk
## 1. Algorithm Introduction
MetaPath2Vec is a graph embedding algorithm for heterogeneous graph, it mainly contains two steps: 1. produce multiple node sequences by randomly walking along abide by the given meta-path; 2. obtain each node's graph embedding by training a word2vec. The first step of random walking is introduced below.

## 2. Parameters
#### IO Params

- input：hdfs path for a graph, could be weighted, each row represents an edge in the form of `srcId | dstId`
- output: hdfs path for output, each row represents the vertex and the corresponding hindex/gindex/windex value in the form of `nodeId | hindex`
- isWeighted: whether the input graph is weighted, default weight is 1.0 when it is set as false
- sep: the separation in input file to separate the srcId and dstId, could be tab, space or comma
- metaPath：must be symmetical, eg."0-1-2-1-0"
- nodeTypePath：hdfs path of node type, two columns: `nodeId nodeType`, nodeType is represented as an integer

#### Algo Params

- walkLength：length of each sequence
- numWalks：number of walk runs, indicates the times of a node is regarded as the starting point for the random walking
- needReplicateEdge：whether to transform the input graph to be undirected 
- partitionNum：num of RDD partitions
- psPartitionNum：num of data partitions on ps
- useBalancePartition：whether to user balancePartition strategy, `true` / `false`, `true` is suggested when the distribution of graph vertices is unbalanced
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
  --name "metapath angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class org.apache.spark.angel.examples.graph.MetaPath2VecExample \
  ../lib/spark-on-angel-examples-3.2.0.jar
  input:$input output:$output sep:tab nodeTypePath:$nodeTypePath metaPath:0-1-2-1-0\
  storageLevel:MEMORY_ONLY useBalancePartition:true \
  partitionNum:4 psPartitionNum:1
```
