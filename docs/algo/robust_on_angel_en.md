# Robust Regression

> Robust Regression is a regression model, which also models the relationship between one or more independent variables and a dependent variable. The difference is that it aims to overcome some limitations of traditional parametric and non-parametric methods, such as misleading results when the assumptions of ordinary least squares are not true, while robust regression is designed not to be overly influenced by the violation of assumptions in the basic data generation process.   

## 1. Introduction

Robust regression is a strong regression method for outliers. Given a data set ![](http://latex.codecogs.com/png.latex?\dpi{100}\displaystyle(Y_i,X_i1,\ldots,X_ip),i=1,\ldots,n) of n statistical units, a linear regression model assumes that the relationship between the dependent variable ![](http://latex.codecogs.com/png.latex?\dpi{100}\displaystyle{Y_i}) and the ![](http://latex.codecogs.com/png.latex?\dpi{100}\displaystyle{X_i1},\ldots,X_ip)  of regressors X is linear. This relationship is modeled through a disturbance term or error variable ε — an unobserved random variable that adds "noise" to the linear relationship between the dependent variable and regressors. However, if the noise is caused by abnormal measurement error or other violations of standard assumptions, then the validity of the conventional linear regression model will be affected. The robust regression model is improved in this respect, and the allowable variance depends on the independent variable X. The model is expressed in the following form:

![model](http://latex.codecogs.com/png.latex?\dpi{150}Y_i=\alpha+\beta_0X_{i1}+\beta_1X_{i2}+\ldots+\beta_pX_{ip}+\varepsilon_i,\qquad&i=1,\ldots,n)  

The robust regression adopts Huber loss function, which divides the residuals into different segments and uses different loss calculation methods for the residuals of different segments.

![model](http://latex.codecogs.com/png.latex?\dpi{150}\L_{\delta}(y-f(x))={\begin{cases}{\frac{1}{2}}\sum_{{i=1}}^{n}{(y_i-f(x_i))^{2}}&{\text{for}}|y_i-f(x_i)|\leq\delta\\\\\delta\sum_{{i=1}}^{n}(|y_i-f(x_i)|-{\frac{1}{2}})&{\text{otherwise.}}\end{cases}})

where ![](http://latex.codecogs.com/png.latex?\dpi{100}\displaystyle(y_{i},x_{i}),({\displaystyle{i}=1,2,\ldots,n)) is a group of samples. This method combine the square loss and absolute loss together to avoid being dominated by particularly large outliers.

## 2. Distributed Implementation on Angel
### 1. Model Storage
Robust regression algorithm can be abstracted as a 1×N PSModel, denoted by w, where ![](http://latex.codecogs.com/png.latex?\dpi{100}w=(\alpha,\beta)), as shown in the following figure:
![](../img/lr_model.png)

### 2. Algorithm Logic
Angel MLLib provides Robust regression algorithm trained with the mini-batch gradient descent method. 

* Worker:    
In each iteration, worker pulls the up-to-date w from PS, updates the model parameters, △w, using the mini-batch gradient descent optimization method, and push △w back to PS. 
* PS:    
In each iteration, PS receives △w from all workers, add their average to w，obtaining a new model.    
  * Flow:      
  
	![](../img/lr_gradient.png)  
  * Algorithm:
 
	![](../img/RobustRegression_gd.png)  


* Decaying learning rate    
The learning rate decays along iterations as ![](../img/LR_lr_ecay.gif), where:
	* α is the decay rate 
	* T is the epoch

## 3. Execution & Performance

### Input Format

* Data fromat is set in "ml.data.type", supporting "libsvm", "dense" and "dummy" types. For details, see [Angel Data Format](data_format_en.md)
* Model size is  set in "ml.model.size", for some sparse model, there are features that all samples are zero at those indices (invalidate indices), therefore ml.model.size = ml.feature.index.range - number of invalidate indices
* Feature vector's dimension is set in "ml.feature.index.range"


###  Parameters
* Algorithm Parameters 
  * ml.epoch.num: number of iterations
  * ml.num.update.per.epoch: number update in each epoch    
  * ml.data.validate.ratio: proportion of data used for validation, no validation when set to 0    
  * ml.learn.rate: initial learning rate
  * ml.learn.decay: decay rate of the learning rate
  * ml.lr.reg.l1: coefficient of the L1 penalty
  * ml.lr.reg.l2: coefficient of the L2 penalty
  * ml.robustregression.loss.delta：residual difference section point

* I/O Parameters
  * ml.feature.num: number of features
  * ml.data.type: [Angel Data Format](data_format_en.md), supporting "dense" and "libsvm"    
  * angel.save.model.path: save path for trained modelangel.train.data.path: input path for train
  * angel.predict.data.path: input path for predict
  * angel.predict.out.path: output path for predict
  * angel.log.path: save path for the log   
   
* Resource Parameters
  * angel.workergroup.number: number of workers  
  * angel.worker.memory.mb: worker's memory requested in G   
  * angel.worker.task.number: number of tasks on each worker, default is 1   
  * angel.ps.number: number of PS 
  * angel.ps.memory.mb: PS's memory requested in G   

### **Output** 

###  **Submit Command**    

* **Training Job**

	```java
    ./bin/angel-submit \
        --action.type=train \
        --angel.app.submit.class=com.tencent.angel.ml.core.graphsubmit.GraphRunner \
        --ml.model.class.name=com.tencent.angel.ml.regression.RobustRegression \
        --angel.train.data.path=$input_path \
        --angel.save.model.path=$model_path \
        --angel.log.path=$log_path \
        --ml.model.is.classification=false \
        --ml.robustregression.loss.delta=1.0 \
        --ml.epoch.num=10 \
        --ml.feature.index.range=150361 \
        --ml.data.validate.ratio=0.1 \
        --ml.learn.rate=0.1 \
        --ml.learn.decay=1 \
        --ml.reg.l2=0.001 \
        --ml.data.type=libsvm \
        --ml.model.type=T_FLOAT_DENSE \
        --ml.num.update.per.epoch=10 \
        --ml.worker.thread.num=4 \
        --angel.workergroup.number=2 \
        --angel.worker.memory.mb=5000 \
        --angel.worker.task.number=1 \
        --angel.ps.number=2 \
        --angel.ps.memory.mb=5000 \
        --angel.job.name=robustReg_network \
        --angel.output.path.deleteonexist=true \
    ```

* **IncTraining Job**

	```java
	./bin/angel-submit \
		--action.type=inctrain \
		--angel.app.submit.class=com.tencent.angel.ml.core.graphsubmit.GraphRunner \
		--ml.model.class.name=com.tencent.angel.ml.regression.RobustRegression \
		--angel.train.data.path=$input_path \
		--angel.load.model.path=$model_path \
		--angel.save.model.path=$model_path \
		--angel.log.path=$log_path \
		--ml.data.is.classification=false \
		--ml.model.is.classification=false \
		--ml.robustregression.loss.delta=1.0 \
		--ml.epoch.num=10 \
		--ml.feature.index.range=$featureNum+1 \
		--ml.data.validate.ratio=0.1 \
		--ml.learn.rate=0.1 \
		--ml.learn.decay=1 \
		--ml.reg.l2=0.001 \
		--ml.data.type=libsvm \
		--ml.model.type=T_FLOAT_DENSE \
		--ml.num.update.per.epoch=10 \
		--ml.worker.thread.num=4 \
		--angel.workergroup.number=2 \
		--angel.worker.memory.mb=5000 \
		--angel.worker.task.number=1 \
		--angel.ps.number=2 \
		--angel.ps.memory.mb=5000 \
		--angel.job.name=robustReg_network \
		--angel.output.path.deleteonexist=true
	```

* **Prediction Job**

    ```java
    ./bin/angel-submit \
        --action.type=predict \
        --angel.app.submit.class=com.tencent.angel.ml.core.graphsubmit.GraphRunner \
        --ml.model.class.name=com.tencent.angel.ml.regression.RobustRegression \
        --angel.predict.data.path=$input_path \
        --angel.load.model.path=$model_path \
        --angel.predict.out.path=$predict_path \
        --angel.log.path=$log_path \
        --ml.feature.index.range=150361 \
        --ml.data.type=libsvm \
        --ml.model.type=T_FLOAT_DENSE \
        --ml.worker.thread.num=4 \
        --angel.workergroup.number=2 \
        --angel.worker.memory.mb=5000 \
        --angel.worker.task.number=1 \
        --angel.ps.number=2 \
        --angel.ps.memory.mb=5000 \
        --angel.job.name=robustReg_network \
        --angel.output.path.deleteonexist=true \
    ```

### Performance
* Data: E2006-tfidf, 1.5×10^5 features, 1.6×10^4 samples
* Resources:
	* Angel: executor: 2, 5G memory, 1 task; ps: 2, 5G memory
* Time of 100 epochs:
	* Angel: 22min




