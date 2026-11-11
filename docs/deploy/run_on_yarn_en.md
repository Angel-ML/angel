# Running Angel On Yarn


Angel's YARN deployment reuses an existing Hadoop cluster. YARN schedules the Angel Master and PS containers as well as the Spark driver and executors.

> The cluster must accommodate both Spark and Angel containers. Allow for Spark memoryOverhead, YARN minimum allocation increments and other workloads in addition to configured memory and CPU. There is no fixed minimum memory requirement suitable for every job.

> Besides Yarn, Angel 3.4.0 also supports the STANDALONE mode (no Yarn required), the EMBEDDED mode (PS embedded in Spark executors), and attaching Spark jobs to a pre-launched PS cluster. See [STANDALONE / EMBEDDED / Attach Deployment](standalone_deploy_en.md).


### 1. **Preparing for the run environment**

The environment requirements for running Angel on Yarn include:

* A working Hadoop cluster, including Yarn and HDFS
	* Hadoop 2.x (built with 2.7.3, verified with 2.10.2)

* A client-side gateway for submitting Angel jobs
	* Java 1.8
	* Spark 3.3.x (built with 3.3.1 / Scala 2.12, verified on 3.3.4)
	* Spark jobs can be submitted normally
	* Angel distribution package angel-\<version\>-bin.zip

* Unzip the package and upload the whole directory to HDFS (the jars in `spark.ps.jars` are loaded from HDFS), then set `SPARK_HOME`, `ANGEL_HOME`, `ANGEL_HDFS_HOME` and `ANGEL_VERSION` in `bin/spark-on-angel-env.sh`

	```
	unzip angel-<version>-bin.zip
	hadoop fs -put angel-<version>-bin hdfs://<hdfs name>/<path>/
	```


### 2. **Angel Job Example**

Taking PageRank as an example:

1. **Uploading Data** (this step can be skipped if you already have data in the HDFS, just pay attention to specify the right data format)

	* find the distribution package and its `data` directory, which contains the graph data `data/bc/edge`
	* on HDFS, create a new directory where we are going to upload the data

		```
		hadoop fs -mkdir hdfs://<hdfs name>/test/pagerank_data
		```
	* upload the data 

		```
		hadoop fs -put data/bc/edge hdfs://<hdfs name>/test/pagerank_data
		```
2. **Submitting the Job**

	* After configuring the environment, run `spark-submit` from the distribution's `bin` directory. `bin/SONA-example` provides a similar example; adjust its resources for your cluster.

	> This example configures approximately 7GiB in total: 1GiB for the driver, 2GiB for two executors, 2GiB for two PS instances and the Angel Master's default 2GiB. Allow additional capacity for Spark container overhead and YARN allocation increments, and ensure the queue has sufficient resources.

	> Spark 3.x no longer accepts `--master yarn-cluster`; use `--master yarn --deploy-mode cluster` instead.

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

	**Meaning of the Parameters**

	| Parameter    | Meaning  |
	| --- | --- |
	| spark.ps.jars  | the jars (HDFS paths) that Angel PS depends on, configured in the `spark-on-angel-env.sh` script  |
	| spark.ps.instances | requested total PS, default 1 |
	| spark.ps.cores | requested cores for each PS, default 1 |
	| spark.ps.memory | requested memory for each PS, default 4g |
	| spark.ps.mode | Default `YARN`; `EMBEDDED` requires explicit PS startup/shutdown in the application, not just a configuration change to this example. See [Other Deployment Options](standalone_deploy_en.md) |
	| spark.ps.extraJavaOptions | extra JVM options for PS processes |
	| spark.ps.log.level | log level of Angel master / PS, default `INFO` |
	| spark.ps.out.overwrite | whether to overwrite Angel's temporary output directory, default `true`; does not control algorithm results saved by `GraphIO.save` |
	| spark.angel.master.memory | memory of the Angel master, default 2g |
	| spark.hadoop.angel.* / spark.angel.* | mapped to the corresponding `angel.*` key, e.g. `spark.hadoop.angel.ps.router.type=range` becomes `angel.ps.router.type=range` |
	| resetProp | algorithm parameter |
	| partitionNum | number of data (Spark) partitions |
	| psPartitionNum | number of model partitions on PS |


	For the ease of usage, there are more other parameters that you can configure:

	* system parameters: [system configuration](config_details_en.md)
	* algorithm parameters: [PageRank](../algo/sona/pagerank_on_sona_en.md)
