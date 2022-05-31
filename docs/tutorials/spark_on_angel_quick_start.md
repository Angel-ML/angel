# Spark on Angel 快速入门

Spark on Angel同时支持YARN和Local两种运行模型，从而方便用户在本地调试程序。Spark on Angel的任务本质上是一个Spark的Application，但是多了一个附属的Application。在任务成功提交后，集群上将会出现两个独立的Application，一个是Spark Application， 一个是Angel-PS Application。两个Application不关联，一个Spark on Angel的作业删除，需要用户或者外部系统同时Kill两个。

## 部署流程

1. **安装Spark**
2. **安装Angel**
	1. 解压angel-\<version\>-bin.zip
	2. 配置angel-\<version\>-bin/bin/spark-on-angl-env.sh下的`SPARK_HOME`, `ANGEL_HOME`, `ANGEL_HDFS_HOME`三个环境变量
	3. 将解压后的angel-\<version\>-bin目录上传到HDFS路径

3. 配置环境变量

	- 需要导入环境脚本：source ./spark-on-angel-env.sh
	- 要配置好Jar包位置：spark.ps.jars=\$SONA_ANGEL_JARS和--jars \$SONA_SPARK_JARS
	- 配置Angel PS需要的资源参数：spark.ps.instance, spark.ps.cores, spark.ps.memory

## 提交任务

完成Spark on Angel的程序编写打包后，可以通过spark-submit的脚本提交任务。不过，有以下几个需要注意的地方：


## 运行Example（PageRank）

```bash
#! /bin/bash
- cd angel-<version>-bin/bin; 
- ./SONA-example
```

脚本内容如下：

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

> 注意要指定Angel PS的资源参数：spark.ps.instance，spark.ps.cores，spark.ps.memory


## PageRank代码片段

[完整代码](https://github.com/Tencent/angel/blob/branch-3.2.0/spark-on-angel/examples/src/main/scala/com/tencent/angel/spark/examples/cluster/PageRankExample.scala)

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

