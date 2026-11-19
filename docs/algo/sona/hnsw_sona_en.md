# HNSW

## 1. Algorithm Introduction
>HNSW is a approximate K-nearest neighbor(ANN) search algorithm based on navigable small world
graphs with controllable hierarchy, also see in introduction of [Hierarchical Navigable Small World graphs](https://arxiv.org/abs/1603.09320).

## 2. Running example

### Algorithm IO parameters

- vectorPath: The path is configured as an HDFS path. Each line is composed of node and vector which is to build Small
  World Graph.
- queryPath: The path is configured as an HDFS path. Each line is composed of node and vector which is to find nearest
  neighbor when 'isInternal' is false,
  as mentioned below.
- outputPath: The path configured as an HDFS path of nearest neighbor search result is to save.
- itemSep: The separating symbols used in vector and query data, include `space`, `comma`, `tab` and `colon`. By
  default, it is
  `colon`.
- vecSep: The separating symbols used in vector value, include `space`, `comma`, `tab` and `colon`. By default, it
  is `space`.
- saveItemSep: The separating symbols used in node nearest neighbor search result, include `space`, `comma` and `tab`.
  By
  default, it is `tab`.

### Algorithm parameters

- psPartitionNum: The number of model partitions is preferably an integer multiple of the number of parameter servers,
  so
  that the number of partitions carried by each ps is equal, and the load of each PS is balanced as much as possible. If
  the amount of data is large, more than 500 is recommended.
- dataPartitionNum: Dataset partitions of input data is generally set to 3-4 times the number of spark executors
  times
  the number of executor cores.
- batchSize: Size of node to search nearest neighbors per batch, it will be applied in PS data conversion and similarity
  compute.
- topK: Number of nearest neighbors of query nodes.
- isInternal: Query path node will be used of query data and the path should not be empty, while it is value configured
  as true.
    - distanceFunction: The function is to compute distance similarity, which includes.
      `cosine-distance`、`l1-distance`、`l2-distance` and `jaccard-distance`, between nodes.
- sheetsNum: The numerical value is referred to concurrency level on computing similarity and also to represent search
  topK neighbors on executor.
- queryPartitionNum: Query dataset partitions.
- storageLevel: Dataset storage
  level([details](https://spark.apache.org/docs/0.8.1/api/core/org/apache/spark/storage/StorageLevel$.html)), the
  default is `MEMORY_ONLY`.
- ef: It is integer to represent nearest neighbor candidates of the query node.
- efConstruction: As similar with `ef`, this is a candidate node set which are constituted as Small World Graph nodes on
  same layer.
- M: Number of connections on each node of Small World Graph.
- maxM: Max connections of each node on each layer except first.
- maxM0: Max connections of each node on first layer.
- mL: Random seed for layer nodes.

### Resource parameters

- Angel PS Config: The product of `ps.instance` and `ps.memory` is the total configuration memory of ps. In order
  to ensure that Angel does not hang, you need to configure memory about twice the size of the model. For PageRank, the
  calculation formula of the model size is: number of `nodes * 3 * 4` Byte, according to which you can estimate the size
  of ps memory that needs to be configured under Graph input of different sizes
- Spark Config：The product of num-executors and executor-memory is the total configuration memory of executors, and
  it is best to store twice the input data. If the memory is tight, 1 times is also acceptable, but relatively slower.
  For example, a `10` billion edge set is about 160G in size, and a `20G * 20` configuration is sufficient. In a
  situation
  where resources are really tight, try to increase the number of partitions!

### Submitting scripts

```
vectorPath=hdfs://my-hdfs/nodeToVector
queryPath=hdfs://my-hdfs/queryNodeToVertor
outputPath=hdfs://my-hdfs/output

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
  --class com.tencent.angel.spark.examples.cluster.HnswExample \
  ../lib/spark-on-angel-examples-3.4.0.jar
  vectorPath:$vectorPath queryPath:$queryPath outputPath:$outputPath itemSep:colon \
  vecSep:space saveItemSep:tab storageLevel:MEMORY_ONLY partitionNum:4 psPartitionNum:1 \
  distanceFunction:cosine-distance queryPartitionNum:4 ef:40 efConstruction:40 M:16 maxM:16 maxM0:32 mL:1.0
```

### FAQ

- At about 10 minutes, the task hangs: The most likely reason is that Angel cannot apply for resources! Since HNSW
  is developed based on Spark On Angel, it actually involves two systems, Spark and Angel, and their application for
  resources from Yarn is independently conducted. After the Spark task is started, Spark submits the Angel task to Yarn.
  If the resource cannot be applied for within a given time, a timeout error will be reported and the task will hang!
  The solution is: 1) Confirm that the resource pool has sufficient resources 2) Add spark conf:
  `spark.hadoop.angel.am.appstate.timeout.ms = xxx` to increase the timeout time, the default value is 600000, which is
  10
  minutes
- How to estimate how many Angel resources I need to configure: To ensure that Angel does not hang, you need to
  configure about twice the size of the model memory. In addition, when possible, the smaller the number of ps, the
  smaller the amount of data transmission, but the pressure of a single ps will be greater, requiring certain
  trade-offs.
- Spark resource allocation: Also mainly consider the memory problem, it is best to save twice the input data. If the
  memory is tight, 1 times is also acceptable, but relatively slower. For example, a 10 billion edge set is about 160G
  in size, and a 20G * 20 configuration is sufficient. In a situation where resources are really tight, try to increase
  the number of partitions!

