# Spark on Angel Quick Start

Spark on Angel supports Yarn and Local modes, allowing users to debug the applications on local. A Spark on Angel application is essentially a Spark application with one auxiliary application. Once an application has been successfully submitted, there will be two applications shown on Yarn: the Spark application and the Angel-PS application.

## Deployment Steps
1. Install Spark
2. Unzip angel-\<version\>-bin.zip
3. Set `SPARK_HOME`, `ANGEL_HOME`, `ANGEL_HDFS_HOME` variables in angel-\<version>\-bin/bin/spark-on-angel-env.sh
4. Upload angel-\<version\>-bin dir to the `ANGEL_HDFS_HOME` path

## Submit a Spark on Angel Job
Once a Spark on Angel application has been packaged, it can be launched by the spark-submit script; make sure to do the following:

- source ./spark-on-angel-env.sh
- set location for the jar: spark.ps.jars=$SONA_ANGEL_JARS and --jars $SONA_SPARK_JARS
- set the Angel PS resource parameters: spark.ps.instance, spark.ps.cores, spark.ps.memory


## Running Example (BreezeSGD)

```bash
#! /bin/bash
- cd angel-<version>-bin/bin; 
- ./SONA-example
```

The script is:

```bash
#!/bin/bash

source ./spark-on-angel-env.sh

${SPARK_HOME}/bin/spark-submit \
    --master yarn-cluster \
    --conf spark.ps.jars=$SONA_ANGEL_JARS \
    --conf spark.ps.instances=2 \
    --conf spark.ps.cores=2 \
    --conf spark.ps.memory=2g \
    --jars $SONA_SPARK_JARS\
    --name "PageRank-spark-on-angel" \
    --driver-memory 1g \
    --num-executors 2 \
    --executor-cores 2 \
    --executor-memory 2g \
    --class com.tencent.angel.spark.examples.cluster.PageRankExample \
    ./../lib/spark-on-angel-examples-${ANGEL_VERSION}.jar \
    input:${ANGEL_HDFS_HOME}/data/bc/edge \
    output:${ANGEL_HDFS_HOME} \
    resetProp:0.15
```

## Minimal Example of PageRank in Spark on Angel Verion

[Complete Code](https://github.com/Tencent/angel/blob/branch-3.2.0/spark-on-angel/examples/src/main/scala/com/tencent/angel/spark/examples/cluster/PageRankExample.scala)

```scala
val edges = GraphIO.load(input, isWeighted = isWeight,
      srcIndex = srcIndex, dstIndex = dstIndex,
      weightIndex = weightIndex, sep = sep)

    val ranks = version match {
      case "edge-cut" => edgeCutPageRank(edges, partitionNum, psPartitionNum,
        storageLevel, tol, resetProp, isWeight,
        useBalancePartition, balancePartitionPercent, numBatch, batchSize)
      case "vertex-cut" => vertexCutPageRank(edges, partitionNum, psPartitionNum,
        storageLevel, tol, resetProp, isWeight,
        useBalancePartition, balancePartitionPercent, numBatch, batchSize)
    }

    GraphIO.save(ranks, output)
```
