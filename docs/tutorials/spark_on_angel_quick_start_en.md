# Spark on Angel Quick Start

Spark on Angel supports Yarn and Local modes, allowing users to debug the applications on local. A Spark on Angel application is essentially a Spark application with one auxiliary application. Once an application has been successfully submitted, there will be two applications shown on Yarn: the Spark application and the Angel-PS application.

## Deployment Steps
1. Install Spark
2. Unzip angel-\<version\>-bin.zip
3. Set `SPARK_HOME`, `ANGEL_HOME`, `ANGEL_HDFS_HOME` variables in angel-<version>-bin/bin/spark-on-angel-env.sh
4. Upload angel-\<version\>-bin dir to the HDFS path

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

$SPARK_HOME/bin/spark-submit \
    --master yarn-cluster \
    --conf spark.ps.jars=$SONA_ANGEL_JARS \
    --conf spark.ps.instances=10 \
    --conf spark.ps.cores=2 \
    --conf spark.ps.memory=6g \
    --jars $SONA_SPARK_JARS\
    --name "LR-spark-on-angel" \
    --driver-memory 10g \
    --num-executors 10 \
    --executor-cores 2 \
    --executor-memory 4g \
    --class com.tencent.angel.spark.examples.basic.LR \
    ./../lib/spark-on-angel-examples-${ANGEL_VERSION}.jar \
    input:<input_path> \
    lr:0.1 \
```

## Minimal Example of LR in Spark on Angel Verion

[Complete Code](https://github.com/Tencent/angel/blob/branch-1.3.0/spark-on-angel/examples/src/main/scala/com/tencent/angel/spark/examples/ml/AngelLR.scala)

```scala
PSContext.getOrCreate(sc)

val psW = PSVector.dense(dim) // weights
val psG = PSVector.duplicate(psW) // gradients of weights

println("Initial psW: " + psW.dimension)
  
for (i <- 1 to ITERATIONS) {
  println("On iteration " + i)
  val localW = psW.pull()
  trainData.map { case (x, label) =>
    val g = x.mul(-label * (1 - 1.0 / (1.0 + math.exp(-label * localW.dot(x)))))
    psG.increment(g)
  }.count()

  psW.toBreeze -= (psG.toBreeze *:* (1.0 / sampleNum))
  psG.reset
}

println(s"Final psW: ${psW.pull().asInstanceOf[IntDoubleVector].getStorage.getValues.mkString(" ")}")
```
