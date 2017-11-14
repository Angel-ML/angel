# Spark on Angel 快速入门

Spark on Angel同时支持YARN和Local两种运行模型，方便用户在本地调试程序。Spark on Angel的任务本质上是一个Spark的Application，但是多了一个附属的Application。在任务成功提交后，YARN集群上，将会出现两个独立的Application，一个是Spark Application， 一个是Angel-PS Application。

## 部署流程
1. 安装Spark运行环境
2.  解压angel-\<version\>-bin.zip
3. 配置angel-\<version\>-bin/bin/spark-on-angl-env.sh下的`SPARK_HOME`, `ANGEL_HOME`, `ANGEL_HDFS_HOME`三个环境变量
4. 将解压后的angel-\<version\>-bin目录上传到HDFS路径

## 提交任务

完成Spark on Angel的程序编写打包后，可以通过spark-submit的脚本提交任务。不过，有以下几个需要注意的地方：

- 需要导入环境脚本：source ./spark-on-angel-env.sh
- 要配置好Jar包位置：spark.ps.jars=\$SONA_ANGEL_JARS和--jars \$SONA_SPARK_JARS
- 指定Angel PS的资源参数：spark.ps.instance，spark.ps.cores，spark.ps.memory

## 运行Example（BreezeSGD）

```bash
#! /bin/bash
- cd angel-<version>-bin/bin; 
- ./SONA-example
```

脚本内容如下：

```bash
#! /bin/bash
source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
    --master yarn-cluster \
    --conf spark.ps.jars=$SONA_ANGEL_JARS \
    --conf spark.ps.instances=10 \
    --conf spark.ps.cores=2 \
    --conf spark.ps.memory=6g \
    --queue g_teg_angel-offline \
    --jars $SONA_SPARK_JARS \
    --name "BreezeSGD-spark-on-angel" \
    --driver-memory 10g \
    --num-executors 10 \
    --executor-cores 2 \
    --executor-memory 4g \
    --class com.tencent.angel.spark.examples.ml.BreezeSGD \
    ./../lib/spark-on-angel-examples-${ANGEL_VERSION}.jar
```


##  Spark on Angel版本的最简LR

[完整代码](https://github.com/Tencent/angel/blob/branch-1.3.0/spark-on-angel/examples/src/main/scala/com/tencent/angel/spark/examples/ml/AngelLR.scala)

```scala
   PSContext.getOrCreate(sc)

   val psW = PSVector.dense(dim)
   val psG = PSVector.duplicate(psW)

   println("Initial psW: " + psW.dimension)

   for (i <- 1 to ITERATIONS) {
     println("On iteration " + i)

     val localW = new DenseVector(psW.pull())

     trainData.map { case (x, label) =>
       val g = -label * (1 - 1.0 / (1.0 + math.exp(-label * localW.dot(x)))) * x
       psG.increment(g.toArray)
     }.count()

     psW.toBreeze -= (psG.toBreeze :* (1.0 / sampleNum))
     psG.zero()
    }

   println(s"Final psW: ${psW.pull().mkString(" ")}")
```
