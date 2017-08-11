# Running Angel On Yarn


Considering that many companies' big data platforms are built with Yarn, we realize Angel's current distributed computing based on Yarn to make it easier to reuse the existing network environment without any changes. 

> Given the procedure and system requirements for building Yarn, we suggest using machines with large memory (for example, >=10G). In general, 6G memory is a minimal requirement for using 1PS+1worker+1am case.


### 1. **Preparing for the run environment**

The environment requirements for running Angel on Yarn include:

* A working Hadoop cluster, including Yarn and HDFS
	* Hadoop >= 2.2.0

* A client-side gateway for submitting Angel jobs
	* Java >= 1.8
	* can submit MR jobs 
	* Angel distribution package angel-\<version\>-bin.zip


### 2. **Angel Job Example**

Taking LogisticRegression as an example:

1. **Uploading Data** (this step can be skipped if you already have data in the HDFS, just pay attention to specify the right data format)

	* find the distribution package and its `data` directory, which contains LogisticRegression data
	* on HDFS, create a new directory where we are going to upload the data

		```
		hadoop fs -mkdir hdfs://<hdfs name>/test/lr_data
		```
	* upload the data 

		```
		hadoop fs -put data/exampledata/LRLocalExampleData/a9a.train hdfs://<hdfs name>/test/lr_data
		```
2. **Submitting the Job**

	* use `angel-submit` under the `bin` directory in the distribution package to submit Angel jobs to the Hadoop cluster

	> **please make sure the cluster has enough resources; for the following example, at least 6GB memory and 3 vcores are needed to start the job**
	
		```bsh
		./angel-submit \
			--angel.app.submit.class com.tencent.angel.example.quickstart.QSLRRunner\
			--angel.train.data.path "hdfs://<hdfs name>/test/lr_data" \
			--angel.log.path "hdfs://<hdfs name>/test/log" \
			--angel.save.model.path "hdfs://<hdfs name>/test/model" \
			--action.type train \
			--ml.data.type dummy \
			--ml.feature.num 1024 \
			--angel.job.name LR_test \
			--angel.am.memory.gb 2 \
			--angel.worker.memory.gb 2 \
			--angel.ps.memory.gb 2
		```

	**Meaning of the Parameters**


	| Parameter    | Meaning  |
	| --- | --- |
	| action.type  | computing type, supporting "train" and "predict" for model training and prediction, respectively  |
	| angel.app.submit.class | the entry point for your application: the run class of an algorithm|
	| angel.train.data.path | training data path |
	| angel.log.path | metrics log path |
	| angel.save.model.path | save path |
	| ml.data.type | data type, supporting libsvm and dummy |
	| ml.feature.num | number of features |
	| angel.job.name | job name |
	| angel.am.memory.gb | requested memory for master |
	| angel.worker.memory.gb | requested memory for each worker |
	| angel.ps.memory.gb | requested memory for one PS|


	For the ease of usage, there are more other parameters that you can configure:

	* system parameters: [system configuration](config_details_en.md)
	* algorithm parameters: [Logistic Regression](../algo/lr_on_angel_en.md)

3. **Monitoring the Progress**


	Once a job is submitted, logs will be printed in the console, such as URL and iteration progress:

	![][1]

	Open the URL, you can see more detailed logs of the progress and algorithm metrics for each component of the Angel job:

	![][2]

	We are currently working to make the monitoring page more user-friendly with better visual presentation. 


  [1]: ../img/angel_client_log.png
  [2]: ../img/lr_worker_log.png
