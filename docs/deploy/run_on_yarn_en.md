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
		hadoop fs -put data/a9a/a9a_123d_train.dummy hdfs://<hdfs name>/test/lr_data
		```
2. **Submitting the Job**

	* use `angel-submit` under the `bin` directory in the distribution package to submit Angel jobs to the Hadoop cluster

	> **please make sure the cluster has enough resources; for the following example, at least 6GB memory and 3 vcores are needed to start the job**
	
	> There are three ways to submit job for algorithms in angel,such as: by parameters, by json or by parameters and json, as for deep learning algorithms can be submitted only by json.
	>
	> If you use both parameters and json to submit job, you must konw parameters have higher priority than json. 

    - **submit by parameters**
	
		```bsh
		./angel-submit \
			--angel.app.submit.class com.tencent.angel.ml.core.graphsubmit.GraphRunner\
			--ml.model.class.name com.tencent.angel.ml.classification.LogisticRegression \
			--angel.train.data.path "hdfs://<hdfs name>/test/lr_data" \
			--angel.log.path "hdfs://<hdfs name>/test/log" \
			--angel.save.model.path "hdfs://<hdfs name>/test/model" \
			--action.type train \
			--ml.data.type dummy \
			--ml.feature.index.range 123 \
			--angel.job.name LR_test \
			--angel.am.memory.gb 2 \
			--angel.worker.memory.gb 2 \
			--angel.ps.memory.gb 2 \
			--ml.epoch.num 5 \
            --ml.data.validate.ratio 0.1 \
            --ml.learn.rate 0.01 \
            --ml.reg.l2 0.03
		```

		- **Meaning of the Parameters**

	
		| Parameter    | Meaning  |
		| --- | --- |
		| action.type  | computing type, supporting "train" and "predict" for model training and prediction, respectively  |
		| angel.app.submit.class | the entry point for your application: the run class of an algorithm|
		| angel.train.data.path | training data path |
		| angel.log.path | metrics log path |
		| angel.save.model.path | save path |
		| angel.job.name | job name |
		| angel.am.memory.gb | requested memory for master |
		| angel.worker.memory.gb | requested memory for each worker |
		| angel.ps.memory.gb | requested memory for one PS|
		| ml.model.class.name | model class name |
		| ml.data.type | data type, supporting libsvm and dummy |
		| ml.feature.index.range | number of features |
		| ml.epoch.num | number of iterations |
		| ml.data.validate.ratio | verification set sampling rate |
		| ml.learn.rate | learning rate |
		| ml.reg.l2 | l2 regularization |
	
	
		For the ease of usage, there are more other parameters that you can configure:

		* system parameters: [system configuration](config_details_en.md)
		* algorithm parameters: [Logistic Regression](../algo/lr_on_angel_en.md)

	- **submit by json**
		>when you submit job by json, you can define the basic parameters in script, and other parameters about algorithm can be defined in json file, as follows:
		
		>*[Notice]:here ```ml.model.class.name``` is ```com.tencent.angel.ml.core.graphsubmit.AngelModel```  
		>```<localpath>/logreg.json``` is the json's path
		
		script:
		```bsh
		./angel-submit \
			--angel.app.submit.class com.tencent.angel.ml.core.graphsubmit.GraphRunner\
			--ml.model.class.name com.tencent.angel.ml.core.graphsubmit.AngelModel \
			--angel.train.data.path "hdfs://<hdfs name>/test/lr_data" \
			--angel.log.path "hdfs://<hdfs name>/test/log" \
			--angel.save.model.path "hdfs://<hdfs name>/test/model" \
			--action.type train \
			--angel.job.name LR_test \
			--angel.am.memory.gb 2 \
			--angel.worker.memory.gb 2 \
			--angel.ps.memory.gb 2 \
			--angel.ml.conf <localpath>/logreg.json \
			--ml.optimizer.json.provider com.tencent.angel.ml.core.PSOptimizerProvider
		```
		
		json file(logreg.json):
		```json
		{
		  "data": {
		    "format": "dummy",
		    "indexrange": 123,
		    "validateratio": 0.1,
		    "sampleratio": 1.0
		  },
		  "train": {
		    "epoch": 10,
		    "lr": 0.5
		  },
		  "model": {
		    "modeltype": "T_FLOAT_SPARSE"
		  },
		  "default_optimizer": {
		    "type": "momentum",
		    "momentum": 0.9,
		    "reg2": 0.001
		  },
		  "layers": [
		    {
		      "name": "wide",
		      "type": "simpleinputlayer",
		      "outputdim": 1,
		      "transfunc": "identity"
		    },
		    {
		      "name": "simplelosslayer",
		      "type": "simplelosslayer",
		      "lossfunc": "logloss",
		      "inputlayer": "wide"
		    }
		  ]
		}
		```
		For the ease of usage, there are more other parameters that you can configure:

		* json parameters: [Json description](../basic/json_conf_en.md)
		
	- **submit by json and parameters**
		>when you submit job using both json and parameters, please pay attention to the following tips:
		
		- ```ml.model.class.name``` is ```com.tencent.angel.ml.core.graphsubmit.AngelModel``` 
		- ```angel.ml.conf``` and ```ml.optimizer.json.provider=com.tencent.angel.ml.core.PSOptimizerProvider``` must be set
		- parameters should be set in the script, and they have higher priority than parameters in json.

3. **Monitoring the Progress**


	Once a job is submitted, logs will be printed in the console, such as URL and iteration progress:

	![][1]

	Open the URL, you can see more detailed logs of the progress and algorithm metrics for each component of the Angel job:

	![][2]

	We are currently working to make the monitoring page more user-friendly with better visual presentation. 


  [1]: ../img/angel_client_log.png
  [2]: ../img/lr_worker_log.png
