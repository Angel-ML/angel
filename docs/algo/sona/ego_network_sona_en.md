# BruteForce

## 1. Algorithm Introduction

> Ego Network consist of a central node, which called as "Ego", and the nodes to whom ego is directly connected to
> reinforce the ties. On the side, the network is named as Person Network on social network analysis. All the nodes but
> central node are called as "Alter". The functions of Ego Network include social support、sense-making、social control
> and access to resources, such as emotional and material aid, way to interpret the world, ensuring that egos behave
> according to norms, the network contacts to get the clients and employees in corporate planning, etc.

## 2. Running example

### Algorithm IO parameters

- input: The path is configured as an HDFS path. Each line of edge is composed of source node and target node.
- output: The path configured as an HDFS path of Ego network result is to save.
- nodePath: The path is configured as an HDFS path. Each line is node identification. The nodes will be regarded as Ego
  nodes whose are all the nodes of edge if this path is not configured.
- needReplicaEdges: The replica edges is to reserve or not.
- sepInNodePath: The separating symbols used to parse node path text line, include `space`, `comma`, `tab` and `colon`.
  By
  default, it is
  `space`.
- sep: The separating symbols used to parse edge text line, include `space`, `comma`, `tab` and `colon`. By default, it
  is `tab`.

### Algorithm parameters

- psPartitionNum: The number of model partitions is preferably an integer multiple of the number of parameter servers,
  so
  that the number of partitions carried by each ps is equal, and the load of each PS is balanced as much as possible. If
  the amount of data is large, more than 500 is recommended.
- dataPartitionNum: Dataset partitions of input data is generally set to 3-4 times the number of spark executors
  times
  the number of executor cores.
- version: value is `v1` or `v2`. All the Alter nodes of Ego node will be outputted on `v1` mode, otherwise nodes with
  triangle will be outputted only on `v2` mode while running in big graph which edge number is over 100 billion.
- batchSize: Size of node to initialize PS neighbor metric.
- pullBatchSize: Size of node to pull PS neighbor metric.
- storageLevel: Dataset storage level, the default value is `MEMORY_ONLY`.
- cpDir: The value is configured as an HDFS path to checkpoint RDD Dataset.

### Resource parameters

- Angel PS Config: The product of `ps.instance` and `ps.memory` is the total configuration memory of ps. In order
  to ensure that Angel does not hang, you need to configure memory about twice the size of the model.
- Spark Config：The product of num-executors and executor-memory is the total configuration memory of executors, and
  it is best to store twice the input data. If the memory is tight, 1 times is also acceptable, but relatively slower.
  For example, a `10` billion edge set is about 160G in size, and a `20G * 20` configuration is sufficient. In a
  situation
  where resources are really tight, try to increase the number of partitions!

### Submitting scripts

```
input=hdfs://my-hdfs/path/of/edge
output=hdfs://my-hdfs/path/of/output
nodePath=hdfs://my-hdfs/path/of/node

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
  ../lib/spark-on-angel-examples-3.3.0.jar
  input:$input output:$output nodePath:$nodePath sep:tab sepInNodePath:space needReplicaEdges:false \
  storageLevel:MEMORY_ONLY partitionNum:4 psPartitionNum:1 batchSize:10000 pullBatchSize:1000
```

### FAQ

- At about 10 minutes, the task hangs: The most likely reason is that Angel cannot apply for resources! Since Ego
  Network
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

