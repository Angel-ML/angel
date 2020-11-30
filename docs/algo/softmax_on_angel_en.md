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
    
## 3. Execution & Performance
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
  
* **Submit Command**
    **Training Job**
	```java
	../../bin/angel-submit \
		-Dml.epoch.num=20 \
		-Dangel.app.submit.class=com.tencent.angel.ml.core.graphsubmit.GraphRunner \
		-Dml.model.class.name=com.tencent.angel.ml.classification.SoftmaxRegression \
		-Dangel.train.data.path=$traindata \
		-Dangel.save.model.path=$modelout \
		-Dml.feature.index.range=$featureNum \
		-Dml.feature.num=$featureNum \
		-Dml.data.validate.ratio=0.1 \
		-Dml.data.label.trans.class="SubOneTrans" \
		-Dml.data.type=libsvm \
		-Dml.learn.rate=0.1 \
		-Dml.learn.decay=0.5 \
		-Dml.reg.l2=0.03 \
		-Daction.type=train \
		-Dml.num.class=$classNum \
		-Dangel.workergroup.number=10 \
		-Dangel.worker.memory.mb=10000 \
		-Dangel.worker.task.number=1 \
		-Dangel.ps.number=4 \
		-Dangel.ps.memory.mb=10000 \
		-Dangel.task.data.storage.level=memory \
		-Dangel.job.name=angel_train
	```

	**IncTraining Job**
	```java
	../../bin/angel-submit \
		-Dml.epoch.num=20 \
		-Dangel.app.submit.class=com.tencent.angel.ml.core.graphsubmit.GraphRunner \
		-Dml.model.class.name=com.tencent.angel.ml.classification.SoftmaxRegression \
		-Dangel.train.data.path=$traindata \
		-Dangel.load.model.path=$modelout \
		-Dangel.save.model.path=$modelout \
		-Dml.feature.index.range=$featureNum \
		-Dml.feature.num=$featureNum \
		-Dml.data.validate.ratio=0.1 \
		-Dml.data.label.trans.class="SubOneTrans" \
		-Dml.data.type=libsvm \
		-Dml.learn.rate=0.1 \
		-Dml.learn.decay=0.5 \
		-Dml.reg.l2=0.03 \
		-Daction.type=inctrain \
		-Dml.num.class=$classNum \
		-Dangel.workergroup.number=10 \
		-Dangel.worker.memory.mb=10000 \
		-Dangel.worker.task.number=1 \
		-Dangel.ps.number=4 \
		-Dangel.ps.memory.mb=10000 \
		-Dangel.task.data.storage.level=memory \
		-Dangel.job.name=angel_inctrain
	```

	**Prediction Job**
	```java
	../../bin/angel-submit \
		-Dml.epoch.num=20 \
		-Dangel.app.submit.class=com.tencent.angel.ml.core.graphsubmit.GraphRunner \
		-Dml.model.class.name=com.tencent.angel.ml.classification.SoftmaxRegression \
		-Dangel.predict.data.path=$predictdata \
		-Dangel.load.model.path=$modelout \
		-Dangel.predict.out.path=$predictout \
		-Dml.feature.index.range=$featureNum \
		-Dml.feature.num=$featureNum \
		-Dml.data.type=libsvm \
		-Daction.type=predict \
		-Dml.num.class=$classNum \
		-Dangel.workergroup.number=10 \
		-Dangel.worker.memory.mb=10000 \
		-Dangel.worker.task.number=1 \
		-Dangel.ps.number=4 \
		-Dangel.ps.memory.mb=10000 \
		-Dangel.task.data.storage.level=memory \
		-Dangel.job.name=angel_predict
	```

### Performance
* data：SVHN，3×10^3 features，7×10^4 samples
* resource：
	* Angel：executor：10，10G memory，1task； ps：4，10G memory
* Time of 100 epochs:
	* Angel：55min