# LR (Logistic Regression)

> Logistic Regression is a regression model where the dependent variable is categorical, thus also a classification model. It is simple but effective, widely used in a variety of applications such as the traditional advertising recommender system.   

## 1. Introduction

Logistic regression is a simple classification method. It assumes that the probability mass of class label y conditional on data point x, P(y|x), takes the logistic form:    

![](../img/LR_P.png)  

Combining the two expressions above, we get:

![](../img/LR_P1.png)  


The objective function of logistic regression is a weighted sum of log loss and L2 penalty:     

![](../img/LR_loss.png)  

where ![](../img/LR_reg.gif) is the regularization term using the L2 norm. 

## 2. Distributed Implementation on Angel
### 1. Model Storage
LR algorithm can be abstracted as a 1×N PSModel, denoted by w, as shown in the following figure:
![](../img/lr_model.png)

### 2. Algorithm Logic
Angel MLLib provides LR algorithm trained with the mini-batch gradient descent method. 

* Worker:    
In each iteration, worker pulls the up-to-date w from PS, updates the model parameters, △w, using the mini-batch gradient descent optimization method, and push △w back to PS. 
* PS:    
In each iteration, PS receives △w from all workers, add their average to w，obtaining a new model.    
  * Flow:      
![](../img/lr_gradient.png)  
  * Algorithm:
![](../img/LR_gd.png)  


* Decaying learning rate    
The learning rate decays along iterations as ![](../img/LR_lr_ecay.gif), where:
	* α is the decay rate 
	* T is the iteration/epoch

* Model Type
The LR algorithm supports three types of models: DoubleDense, DoubleSparse, DoubleSparseLongKey. Use `ml.lr.model.type` to configure. 
	* DoubleDense
		* Parameters: -- ml.lr.model.type: T_DOUBLE_DENSE
		* Description: DoubleDense type model is suitable for dense data; model saved as array to save space; quick access and high performance
	* DoubleSparse
		* Parameters: -- ml.lr.model.type：T_DOUBLE_SPARSE
		* Description: DoubleSparse type model is suitable for sparse data; model saved as map, where K is feature ID and V is feature value; range of K same as range of Int
	* DoubleSparseLongKey
		* Parameters: -- ml.lr.model.type：T_DOUBLE_SPARSE_LONGKEY
		* DoubleSparseLongKey type model is suitable for highly sparse data; model saved as map, where K is feature ID and V is feature value; range of K same as range of Long

## 3. Execution & Performance

### Input Format

* Data fromat is set in "ml.data.type", supporting "libsvm" and "dummy" types. For details, see [Angel Data Format](data_format_en.md)
* Feature vector's dimension is set in "ml.feature.index.range"


###  Parameters
* Algorithm Parameters 
  * ml.epoch.num: number of iterations/epochs
  * ml.model.size: the size of model. for some sparse model, there are features that all samples are zero at those indices (invalidate indices). ml.model.size = ml.feature.index.range - number of invalidate indices 
  * ml.batch.sample.ratio: sampling rate for each iteration   
  * ml.num.update.per.epoch: number update in each iteration    
  * ml.data.validate.ratio: proportion of data used for validation, no validation when set to 0    
  * ml.learn.rate: initial learning rate
  * ml.learn.decay: decay rate of the learning rate
  * ml.lr.reg.l2: coefficient of the L2 penalty

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

###  **Submit Command**    

You can submit job by setting the parameters above one by one in the script or construct network by json file as follows(see [Json description](../basic/json_conf_en.md) for a complete description of the Json configuration file)
- If you use both parameters and json in script, parameters in script have higher priority. 
- If you only use parameters in script you must change `ml.model.class.name` as `--ml.model.class.name com.tencent.angel.ml.classification.LogisticRegression` and do not set this parameter `angel.ml.conf` which is for json file path.
Here we provide an example submitted by using json file.

* **Json file**
 
```json
 {
   "data": {
     "format": "libsvm",
     "indexrange": 1000000,
     "validateratio": 0.1,
     "useshuffle": true
   },
   "train": {
     "epoch": 10,
     "lr": 4.0,
     "numupdateperepoch": 200,
     "decayclass": "StandardDecay",
     "decayalpha": 0.01
   },
   "model": {
     "modeltype": "T_FLOAT_DENSE"
   },
   "default_optimizer": {
      "type": "sgd"
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

* **Training Job**

```shell
runner="com.tencent.angel.ml.core.graphsubmit.GraphRunner"
modelClass="com.tencent.angel.ml.core.graphsubmit.AngelModel"

$ANGEL_HOME/bin/angel-submit \
    --angel.job.name lr \
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
    --angel.ml.conf $lr_json_path \
    --ml.optimizer.json.provider com.tencent.angel.ml.core.PSOptimizerProvider
```

Data: criteo.kaggle2014.train.svm.field, 1×10^6 features, 4×10^7 data points
* Resources:
	* Angel: worker: 20, 30G memory, 2 tasks; ps: 10, 5G memory





