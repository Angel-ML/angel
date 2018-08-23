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

```Scala

   val points:RDD[Point] = _

   val wPS = PSVector.dense(DIM).fill(0.0)
   val gradientPS = PSVector.dense(DIM).fill(0.0)

   for (i <- 1 to ITERATIONS) {
     val totalG = gradientPS.toCache

   val trigger = points.mapPartitions { iter =>
     val brzW = new DenseVector(wPS.pull())

     val subG = iter.map { p =>
       p.x * (1 / (1 + math.exp(-p.y * brzW.dot(p.x))) - 1) * p.y
     }.reduce(_ + _)

     totalG.push(subG.toArray())
     Iterator.empty
      }
     trigger.count()

     wPS.toBreeze += -1.0 * gradientPS.toBreeze
     gradientPS.fill(0.0)
   }

   println("feature sum:" + wPS.pull())

   gradientPS.delete()
   wPS.delete()
  }
```
