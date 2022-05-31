# Angel On Yarn运行


由于业界很多公司的大数据平台，都是基于Yarn搭建，所以Angel目前的分布式运行是基于Yarn，方便用户复用现网环境，而无需任何修改。

> 鉴于Yarn的搭建步骤和机器要求，不建议在小机器上，进行尝试该运行。如果一定要运行，最少需要6G的内存（1ps+1worker+1am），最好有10G的内存，比较宽裕。

### 1. **运行环境准备**

Angel的分布式Yarn运行模式需要的环境，其实也非常简单：

* 一个可以正常运行Hadoop集群，包括Yarn和HDFS
	* Hadoop >= 2.2.0

* 一个用于提交Angel任务的客户端Gateway
	* Java >= 1.8
	* Spark >= 2.4
	* 可以正常提交Spark作业
	* Angel发布包：angel-\<version\>-bin.zip


### 2. **Angel任务运行示例**

以最简单的PageRank为例：

1. **上传数据**（如果用户有自己的数据可以略过本步，但是要确认数据格式一致）

	* 找到发布包的data目录下的LogisticRegression测试数据
	* 在hdfs上新建lr训练数据目录

		```
		hadoop fs -mkdir hdfs://<hdfs name>/test/pagerank_data
		```
	* 将数据文件上传到指定目录下

		```
		hadoop fs -put data/bc/edge hdfs://<hdfs name>/test/pagerank_data
		```
2. **提交任务**

	* 使用`spark-submit`将任务提交到Hadoop集群

	> **请务必注意提交集群中是否有充足的资源，如果按照下面的参数配置，启动任务至少需要6GB内存和4个vcore**
	
		```
		        #!/bin/bash
		        
                input=hdfs://<hdfs name>/test/pagerank_data
                output=hdfs://<hdfs name>/test/pagerank_output
                queue=your_queue_name
                
                source ./spark-on-angel-env.sh
                
                ${SPARK_HOME}/bin/spark-submit \
                    --master yarn-cluster \
                    --conf spark.ps.jars=$SONA_ANGEL_JARS \
                    --conf spark.ps.instances=2 \
                    --conf spark.ps.cores=1 \
                    --conf spark.ps.memory=1g \
                    --jars $SONA_SPARK_JARS\
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
                    resetProp:0.15
		```

	**参数含义如下**


	| 名称    | 含义  |
	| --- | --- |
	| spark.ps.jars  | Angel PS运行依赖的jar包，在`spark-on-angel-env.sh`脚本里面配置  |
    | spark.ps.instances | 申请的Angel PS总数 |
    | spark.ps.cores | 每个PS申请的core |
    | spark.ps.memory | 每个PS申请的内存 |
    | resetProp | 算法参数 |


	为了方便用户，Ange设置有许多参数可供调整，可以参考

	* 系统参数： [主要系统配置](config_details.md)
	* 算法参数： [PageRank](../algo/sona/pagerank_on_angel_en.md)
