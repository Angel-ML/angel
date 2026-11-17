# Angel On Yarn运行


Angel的Yarn部署模式可复用已有Hadoop集群，由Yarn调度Angel Master与PS，以及Spark Driver与Executor。

> 集群需同时容纳Spark与Angel的容器。除了配置的内存和CPU，还要考虑Spark的memoryOverhead、Yarn最小资源分配粒度和其他任务的资源占用；不存在适用于所有作业的固定最低内存。

> 除Yarn外，Angel 3.4.0 还支持不依赖Yarn的STANDALONE模式、PS嵌入Spark Executor的EMBEDDED模式，以及Spark任务attach到已启动PS集群的方式，详见[STANDALONE / EMBEDDED / Attach 部署](standalone_deploy.md)。

### 1. **运行环境准备**

Angel的分布式Yarn运行模式需要的环境，其实也非常简单：

* 一个可以正常运行Hadoop集群，包括Yarn和HDFS
	* Hadoop 2.x（编译版本为 2.7.3，已在 2.10.2 上验证）

* 一个用于提交Angel任务的客户端Gateway
	* Java 1.8
	* Spark 3.3.x（编译版本为 3.3.1，Scala 2.12，已在 3.3.4 上验证）
	* 可以正常提交Spark作业
	* Angel发布包：angel-\<version\>-bin.zip

* 将发布包解压后整体上传到HDFS（`spark.ps.jars`中的jar包需要从HDFS上加载），并修改`bin/spark-on-angel-env.sh`中的`SPARK_HOME`、`ANGEL_HOME`、`ANGEL_HDFS_HOME`、`ANGEL_VERSION`

	```
	unzip angel-<version>-bin.zip
	hadoop fs -put angel-<version>-bin hdfs://<hdfs name>/<path>/
	```


### 2. **Angel任务运行示例**

以最简单的PageRank为例：

1. **上传数据**（如果用户有自己的数据可以略过本步，但是要确认数据格式一致）

	* 找到发布包的data目录下的图测试数据`data/bc/edge`
	* 在hdfs上新建PageRank训练数据目录

		```
		hadoop fs -mkdir hdfs://<hdfs name>/test/pagerank_data
		```
	* 将数据文件上传到指定目录下

		```
		hadoop fs -put data/bc/edge hdfs://<hdfs name>/test/pagerank_data
		```
2. **提交任务**

	* 配置环境后进入发布包`bin`目录，使用`spark-submit`提交到Hadoop集群；`bin/SONA-example`提供同类示例，资源参数可按实际集群调整。

	> 本例配置内存合计约7GiB（Driver 1GiB、两个Executor共2GiB、两个PS共2GiB、Angel Master默认2GiB）。调度时还需考虑Spark容器的额外内存及Yarn资源分配粒度，请确认队列资源足够。

	> Spark 3.x 不再支持 `--master yarn-cluster`，请使用 `--master yarn --deploy-mode cluster`。

	```bash
	#!/bin/bash

	input=hdfs://<hdfs name>/test/pagerank_data
	output=hdfs://<hdfs name>/test/pagerank_output
	queue=your_queue_name

	source ./spark-on-angel-env.sh

	${SPARK_HOME}/bin/spark-submit \
	    --master yarn \
	    --deploy-mode cluster \
	    --conf spark.executor.extraJavaOptions=-XX:+UseG1GC \
	    --conf spark.ps.jars=$SONA_ANGEL_JARS \
	    --conf spark.ps.instances=2 \
	    --conf spark.ps.cores=1 \
	    --conf spark.ps.memory=1g \
	    --jars $SONA_SPARK_JARS \
	    --name "PageRank-spark-on-angel" \
	    --queue $queue \
	    --driver-memory 1g \
	    --num-executors 2 \
	    --executor-cores 1 \
	    --executor-memory 1g \
	    --class com.tencent.angel.spark.examples.cluster.PageRankExample \
	    ./../lib/spark-on-angel-examples-${ANGEL_VERSION}.jar \
	    input:$input \
	    output:$output \
	    resetProp:0.15 \
	    partitionNum:4 \
	    psPartitionNum:4
	```

	**参数含义如下**

	| 名称    | 含义  |
	| --- | --- |
	| spark.ps.jars  | Angel PS运行依赖的jar包（HDFS路径），在`spark-on-angel-env.sh`脚本里面配置  |
	| spark.ps.instances | 申请的Angel PS总数，默认1 |
	| spark.ps.cores | 每个PS申请的core，默认1 |
	| spark.ps.memory | 每个PS申请的内存，默认4g |
	| spark.ps.mode | 默认`YARN`；`EMBEDDED`需应用显式启停PS，不能仅修改本示例的配置，见[其他部署方式](standalone_deploy.md) |
	| spark.ps.extraJavaOptions | PS进程的额外JVM参数 |
	| spark.ps.log.level | Angel Master/PS 的日志级别，默认`INFO` |
	| spark.ps.out.overwrite | Angel临时输出目录存在时是否覆盖，默认`true`；不控制`GraphIO.save`的算法结果输出 |
	| spark.angel.master.memory | Angel Master内存，默认2g |
	| spark.hadoop.angel.* / spark.angel.* | 转换为对应的`angel.*`键传入Angel，例如`spark.hadoop.angel.ps.router.type=range`对应`angel.ps.router.type=range` |
	| resetProp | 算法参数 |
	| partitionNum | 数据（Spark）分区数 |
	| psPartitionNum | PS上模型分区数 |


	为了方便用户，Angel设置有许多参数可供调整，可以参考

	* 系统参数： [主要系统配置](config_details.md)
	* 算法参数： [PageRank](../algo/sona/pagerank_on_sona.md)
