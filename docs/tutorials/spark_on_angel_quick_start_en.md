# Spark on Angel Quick Start

## Deployment Steps
- Install Spark 
- Unzip angel-\<version\>-bin.zip
- Upload angel-\<version\>-bin dir to the HDFS path
- Set SPARK_HOME, ANGEL_HOME, ANGEL_HDFS_HOME variables in angel-<version>-bin/bin/spark-on-angel-env.sh

## Running Examples
- cd angel-\<version\>-bin/bin; ./SONA-example

```bash
#! /bin/bash
source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
    --master yarn-cluster \
    --conf spark.ps.jars=$SONA_ANGEL_JARS \
    --conf spark.ps.instances=10 \
    --conf spark.ps.cores=2 \
    --conf spark.ps.memory=6g \
    --queue g_teg_angel.g_teg_angel-offline \
    --jars $SONA_SPARK_JARS \
    --name "BreezeSGD-spark-on-angel" \
    --driver-memory 10g \
    --num-executors 10 \
    --executor-cores 2 \
    --executor-memory 4g \
    --class com.tencent.angel.spark.examples.ml.BreezeSGD \
    ./../lib/spark-on-angel-examples-${ANGEL_VERSION}.jar
```

## How to submit a Spark on Angel Job
Spark on Angel job is essentially a Spark application. After a Spark on Angel application is bundled, it can be launched by the spark-submit script; however, there are a few differences：
- source ./spark-on-angel-env.sh
- set spark.ps.jars=$SONA_ANGEL_JARS and --jars $SONA_SPARK_JARS
- spark.ps.instance，spark.ps.cores，spark.ps.memory are the resource-allocation variables for Angel PS

Once you have successfully submitted your job，YARN will show two applications: the Spark application and the Angel-PS application

## Supported Modes
Support both YARN mode and Local mode

## Example Code: Implementing Gradient Descent with Angel PS

A simple example is shown below

```java
val w = PSVector.dense(dim)
val sc = SparkSession.builder().getOrCreate().sparkContext

for (i <- 1 to ITERATIONS) {
 val bcW = sc.broadcast(w.pull())
 val totalG = PSVector.duplicate(w)

 val tempRDD = trainData.mapPartitions { iter =>
   val breezeW = new DenseVector(bcW.value)

   val subG = iter.map { case (feat, label) =>
     val brzData = new DenseVector[Double](feat.toArray)
     val margin: Double = -1.0 * breezeW.dot(brzData)
     val gradientMultiplier = (1.0 / (1.0 + math.exp(margin))) - label
     val gradient = brzData * gradientMultiplier
     gradient
   }.reduce(_ + _)
   totalG.increment(subG.toArray)
   Iterator.empty
 }
 tempRDD.count()
 w.toBreeze -= (totalG.toBreeze :* (1.0 / sampleNum))
}

println(s"w: ${w.pull().mkString(" ")}")
```

