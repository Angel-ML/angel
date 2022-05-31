# Running Angel On Yarn


Considering that many companies' big data platforms are built with Yarn, we realize Angel's current distributed computing based on Yarn to make it easier to reuse the existing network environment without any changes. 

> Given the procedure and system requirements for building Yarn, we suggest using machines with large memory (for example, >=10G). In general, 6G memory is a minimal requirement for using 1PS+1worker+1am case.


### 1. **Preparing for the run environment**

The environment requirements for running Angel on Yarn include:

* A working Hadoop cluster, including Yarn and HDFS
	* Hadoop >= 2.2.0

* A client-side gateway for submitting Angel jobs
	* Java >= 1.8
	* Spark >= 2.4.0 
	* Angel distribution package angel-\<version\>-bin.zip


### 2. **Angel Job Example**

Taking PageRank as an example:

1. **Uploading Data** (this step can be skipped if you already have data in the HDFS, just pay attention to specify the right data format)

	* find the distribution package and its `data` directory, which contains graph data
	* on HDFS, create a new directory where we are going to upload the data

		```
		hadoop fs -mkdir hdfs://<hdfs name>/test/pagerank_data
		```
	* upload the data 

		```
		hadoop fs -put data/bc/edge hdfs://<hdfs name>/test/pagerank_data
		```
2. **Submitting the Job**

	* use `spark-submit` to submit Angel jobs to the Hadoop cluster

	> **please make sure the cluster has enough resources; for the following example, at least 5GB memory and 4 vcores are needed to start the job**
	
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

	**Meaning of the Parameters**


	| Parameter    | Meaning  |
	| --- | --- |
	| spark.ps.jars  | the jar package that angel ps depends on is configured in the xx script  |
	| spark.ps.instances | requested total PS |
	| spark.ps.cores | requested cores for each PS |
	| spark.ps.memory | requested memory for each PS |
	| angel.save.model.path | save path |
	| resetProp | algorithm parameters |


	For the ease of usage, there are more other parameters that you can configure:

	* system parameters: [system configuration](config_details_en.md)
	* algorithm parameters: [PageRank](../algo/sona/pagerank_on_angel_en.md)
