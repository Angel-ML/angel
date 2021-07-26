# Swing
## 1. Algorithm Introduction
Swing is a similarity calculating method for "user-item" bipartite graph. Take the purchase graph for an example, the less two users' common purchases of items, the more similar these items are. The detailed formula is as below, in which 'Ui' indicates users purchased item i, "Iu" indicates the items that user u purchased, the value range for gamma is [-1, 0), indicates the penalty for large item set.  

![swing](../../img/swing.png)

## 2. Parameters
#### IO Params

- input：hdfs path for a "user-item" unweighted bipartite graph, each row represents an edge in the form of `userId | itemId`
- output: hdfs path for output, each row represents a pair of item and the corresponding similarity score: `itemId itemId score`
- sep: the separation in input file to separate the srcId and dstId, could be tab, space or comma

#### Algo Params

- topFrom: sort the items by their popularity (number of edges to users), pick out items within the range of [topFrom,topTo), and calculate the similarities between these items, this is useful when only similarity scores between less popular items are wanted
- topTo: refere to "topFrom"
- alpha: refer to the formula and the introduction, default value is 0
- beta: refer to the formula and the introduction, default value is 5
- gamma: refer to the formula and the introduction, default value is -0.3
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
  --name "swing angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class org.apache.spark.angel.examples.graph.SwingExample \
  ../lib/spark-on-angel-examples-3.2.0.jar
  input:$input output:$output sep:tab storageLevel:MEMORY_ONLY useBalancePartition:true \
  partitionNum:4 psPartitionNum:1
```
