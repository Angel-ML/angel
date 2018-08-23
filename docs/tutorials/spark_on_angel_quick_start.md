# Spark on Angel 快速入门

## 部署流程
- 安装Spark运行环境
- 解压angel-\<version\>-bin.zip
- 将解压后的angel-\<version\>-bin目录上传到HDFS路径
- 配置angel-\<version\>-bin/bin/spark-on-angl-env.sh下的SPARK_HOME, ANGEL_HOME, ANGEL_HDFS_HOME三个环境变量

## 运行example
- cd angel-<version>-bin/bin; ./SONA-example

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

## 提交Spark on Angel任务
Spark on Angel的任务本质上是一个Spark的Application，完成Spark on Angel的程序编写打包后，通过spark-submit的脚本提交任务。
不过，Spark on Angel提交的脚本有以下几个不同的地方：
- source ./spark-on-angel-env.sh
- 配置spark.ps.jars=$SONA_ANGEL_JARS和--jars $SONA_SPARK_JARS
- spark.ps.instance，spark.ps.cores，spark.ps.memory是配置Angel PS的资源参数

任务成功提交后，YARN将会出现两个Application，一个是Spark Application， 一个是Angel-PS Application。

## 支持运行模式
同时支持YARN和Local两种运行模型，方便用户在本地调试程序

## Example Code: Gradient Descent的Angel PS实现

下面是一个简单版本的Gradient Descent的PS实现
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
