# SoftMax Regression      
> Softmax regression is also known as multivariate logistic regression, which deals with multi-classification problems through logistic regression algorithms. Unlike binary logistic regression, softmax can handle the classification problem of K categorical variables.

## 1. Introduction
### SoftMax Regression     
* SoftMax Regression Model

Given the test input x, it is assumed that the probability P(y = k|x) can be estimated of different k values for the same sample (where k = 1, ..., K). It is assumed that the output is a K-dimensional vector(the sum of the vector is 1), which represents the estimated probability value corresponding to K categories. Assume that the probability estimation function as follows:
![](../img/SoftMax_p.png)

where，![](../img/SoftMax_exp.png)

The loss function expression is as follows:
![](../img/SoftMax_loss.png)

We also usually solve the above expressions in an iterative manner. Here is the gradient expression for the iterative process:
![](../img/SoftMax_grad.png)

## 2. SoftMax on Angel
* Softmax Algorithm Model
The softmax algorithm consists of only a single input layer, which can be "dense" or "sparse". Like the logistic regression algorithm, the softmax model only has one output which is the classification result. The diferent point is that softmax's loss function uses softmax loss.

* softmax regression training process
    Angel implements the gradient descent method optimization, and gets the model by iterative training. The logic of each iteration of the worker and PS is as follows:       
    * worker：Each iteration pulls the weight vector corresponding to the matrix from the PS to the local, and calculates the update value of the corresponding gradient, then pushes it to the gradient vector corresponding to the PS. Note that the corresponding model weights are pulled according to the feature index corresponding to each sample on the worker side, which can reduce the communication cost and save the memory to improve the calculation efficiency.
    * PS：PS summarizes the gradient update values pushed by all workers, averages them, calculates new model weights through the optimizer and updates accordingly.
    
* softmax regression predict
    * format：rowID,pred,prob,label
    * note：
	    * rowID indicates the row ID of the sample, counting from 0; 
	    * pred: the predicted value of the sample; 
	    * prob: the probability that the sample is relative to the predicted result; 
	    * label: the category to which the predicted sample is classified, and the sample is divided into one category which has the maximum probability, the category label starts counting from 0
    
## 3. Execution
### Input Format
Data fromat is set in "ml.data.type", supporting "libsvm", "dense" and "dummy" types. For details, see [Angel Data Format](data_format_en.md)
where the "libsvm" format is as follows:
 ```
 1 1:1 214:1 233:1 234:1
 ```   
 where the "dummy" format is as follows：
    
 ```
 1 1 214 233 234
 ```

### Parameters
* Algorithm Parameters            
	* ml.epoch.num：number update in each epoch 
    * ml.feature.index.range:thr index range of features
    * ml.feature.num： number of features
    * ml.data.validate.ratio：proportion of data used for validation, no validation when set to 0 
    * ml.data.type：* ml.data.type: [Angel Data Format](data_format_en.md), supporting "dense" and "libsvm"  
    * ml.learn.rate：initial learning rate
    * ml.learn.decay：decay rate of the learning rate
    * ml.reg.l2:coefficient of the L2 penalty
    * action.type：the type of job，supporting "train","inctrain" and "predict"
    * ml.num.class：number of categories
    * ml.sparseinputlayer.optimizer：the type of optimizer，supporting“adam”,"ftrl" and “momentum”
    * ml.data.label.trans.class：Process input data category labels, optional：
	    * "notrans", not transform(default)
	    * "posnegtrans", transform as（-1,1）
	    * "zeroonetrans", transform as（0,1）
	    * "addonetrans", all labels add 1
	    * "subonetrans", all label sub 1
 
###  **Submit Command**    
  
you can submit job by setting the parameters above one by one in the script or construct network by json file as follows (see [Json description](../basic/json_conf_en.md) for a complete description of the Json configuration file)
- If you use both parameters and json in script, parameters in script have higher priority.
- If you only use parameters in script you must change `ml.model.class.name` as `--ml.model.class.name com.tencent.angel.ml.classification.SoftmaxRegression` and do not set this parameter `angel.ml.conf` which is for json file path.
here we provide an example submitted by using json file(see [data](https://github.com/Angel-ML/angel/tree/master/data/protein))

```json
{
  "data": {
    "format": "libsvm",
    "indexrange": 357,
    "validateratio": 0.1
  },
  "model": {
    "modeltype": "T_DOUBLE_SPARSE",
    "modelsize": 357
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
      "outputdim": 3,
      "transfunc": "identity"
    },
    {
      "name": "softmaxlosslayer",
      "type": "simplelosslayer",
      "lossfunc": "softmaxloss",
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
	    --angel.job.name softmax \
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
	    --angel.ml.conf $softmax_json_path \
	    --ml.optimizer.json.provider com.tencent.angel.ml.core.PSOptimizerProvider
	```

	**Prediction Job**

	```shell
	runner="com.tencent.angel.ml.core.graphsubmit.GraphRunner"
	modelClass="com.tencent.angel.ml.core.graphsubmit.AngelModel"
	
	$ANGEL_HOME/bin/angel-submit \
	    --angel.job.name softmax \
	    --action.type predict \
	    --angel.app.submit.class $runner \
	    --ml.model.class.name $modelClass \
	    --angel.predict.data.path $input_path \
	    --angel.load.model.path $model_path \
	    --angel.predict.out.path=$predictout \
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
	    --angel.ml.conf $softmax_json_path \
	    --ml.optimizer.json.provider com.tencent.angel.ml.core.PSOptimizerProvider
	```
