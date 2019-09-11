# Support Vector Machine (SVM)

> SVM is used for classification and regression analysis. 


## 1. Introduction
SVM solves the following optimization problem: 

![](../img/SVM_obj.png)

where
![](../img/SVM_reg.png)
is the regularization term;
![](../img/SVM_lambda.png)
is the regularization coefficient;
![](../img/SVM_hingeloss.png) is the hinge loss as visualized below:  

![](../img/SVM_hingeloss_pic.png)


## 2. Distributed Implementation on Angel
Angel MLLib uses mini-batch gradient descent optimization method for solving SVM's objective; the algorithm is shown below: 

![](../img/SVM_code.png)



## 3. Execution
### Input Format

* Data fromat is set in "ml.data.type", supporting "libsvm" and "dummy" types. For details, see [Angel Data Format](data_format_en.md)

* Feature vector's dimension is set in "ml.feature.num"

### Parameters
* Algorithm Parameters
  * ml.epoch.num: number of epochs
  * ml.batch.sample.ratio: sampling rate for each epoch
  * ml.num.update.per.epoch: number of mini-batches in each epoch
  * ml.data.validate.ratio: proportion of data used for validation, no validation when set to 0
  * ml.learn.rate: initial learning rate
  * ml.learn.decay: decay rate of the learning rate
  * ml.svm.reg.l2: coefficient of the L2 penalty

* I/O Parameters
  * angel.train.data.path: input path for train
  * angel.predict.data.path: input path for predict
  * ml.feature.num: number of features
  * ml.data.type: [Angel Data Format](data_format_en.md), supporting "dummy" and "libsvm" 
  * angel.save.model.path: save path for trained model
  * angel.predict.out.path: output path for predict
  * angel.log.path: save path for the log
 
* Resource Parameters
  * angel.workergroup.number: number of workers
  * angel.worker.memory.mb: worker's memory requested in G
  * angel.worker.task.number: number of tasks on each worker, default is 1
  * angel.ps.number: number of PS
  * angel.ps.memory.mb: PS's memory requested in G

### Submit Command    
  
You can submit job by setting the parameters above one by one in the script or construct network by json file as follows (see [Json description](../basic/json_conf_en.md) for a complete description of the Json configuration file)
- If you use both parameters and json in script, parameters in script have higher priority.
- If you only use parameters in script you must change `ml.model.class.name` as `--ml.model.class.name com.tencent.angel.ml.classification.SupportVectorMachine` and do not set this parameter `angel.ml.conf` which is for json file path.
Here we provide an example submitted by using json file(see [data](https://github.com/Angel-ML/angel/tree/master/data/census))

```json
{
  "data": {
    "format": "dummy",
    "indexrange": 148,
    "numfield": 13,
    "validateratio": 0.1
  },
  "model": {
    "modeltype": "T_DOUBLE_SPARSE_LONGKEY",
    "modelsize": 148
  },
  "train": {
    "epoch": 10,
    "numupdateperepoch": 10,
    "lr": 0.1,
    "decay": 0.8
  },
   "default_optimizer": {
    "type": "momentum",
    "momentum": 0.9,
    "reg2": 0.01
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
      "lossfunc": "hingeloss",
      "inputlayer": "wide"
    }
  ]
}

```


* **Submit Command**
    **Training Job**
	
	```shell
	runner="com.tencent.angel.ml.core.graphsubmit.GraphRunner"
	modelClass="com.tencent.angel.ml.core.graphsubmit.AngelModel"
	
	$ANGEL_HOME/bin/angel-submit \
	    --angel.job.name svm \
	    --action.type train \
	    --angel.app.submit.class $runner \
	    --ml.model.class.name $modelClass \
	    --angel.train.data.path $input_path \
	    --angel.save.model.path $model_path \
	    --angel.log.path $log_path \
	    --angel.workergroup.number $workerNumber \
	    --angel.worker.memory.gb $workerMemory  \
	    --angel.worker.task.number $taskNumber \
	    --angel.ps.number $PSNumber \
	    --angel.ps.memory.gb $PSMemory \
	    --angel.output.path.deleteonexist true \
	    --angel.task.data.storage.level $storageLevel \
	    --angel.task.memorystorage.max.gb $taskMemory \
	    --angel.worker.env "LD_PRELOAD=./libopenblas.so" \
	    --angel.ml.conf $svm_json_path \
	    --ml.optimizer.json.provider com.tencent.angel.ml.core.PSOptimizerProvider
	```

	**Prediction Job**
	
	```shell
	runner="com.tencent.angel.ml.core.graphsubmit.GraphRunner"
	modelClass="com.tencent.angel.ml.core.graphsubmit.AngelModel"
	
	$ANGEL_HOME/bin/angel-submit \
	    --angel.job.name svm \
	    --action.type predict \
	    --angel.app.submit.class $runner \
	    --ml.model.class.name $modelClass \
	    --angel.predict.data.path $input_path \
	    --angel.load.model.path $model_path \
	    --angel.predict.out.path $predictout \
	    --angel.log.path $log_path \
	    --angel.workergroup.number $workerNumber \
	    --angel.worker.memory.gb $workerMemory  \
	    --angel.worker.task.number $taskNumber \
	    --angel.ps.number $PSNumber \
	    --angel.ps.memory.gb $PSMemory \
	    --angel.output.path.deleteonexist true \
	    --angel.task.data.storage.level $storageLevel \
	    --angel.task.memorystorage.max.gb $taskMemory \
	    --angel.worker.env "LD_PRELOAD=./libopenblas.so" \
	    --angel.ml.conf $svm_json_path \
	    --ml.optimizer.json.provider com.tencent.angel.ml.core.PSOptimizerProvider
	```

