# LogisticRegression

> 逻辑回归模型（logistic regression model）是一种分类模型。它是最常见和常用的一种分类方法，在传统的广告推荐中被大量使用，朴实但有效。

## 1. 算法介绍

逻辑回归模型（logistic regression model）是一种分类模型。样本x属于类别y的概率P(y|x)服从logistic分布：   

![](../img/LR_P.png)  

综合两种情况，有：      

![](../img/LR_P1.png)  


逻辑回归模型使用log损失函数，带L2惩罚项的目标函数如下所示：    

![](../img/LR_loss.png)  

其中：![](../img/LR_reg.gif)为L2正则项。

## 2. 分布式实现 on Angel

Angel MLLib提供了用Mini-Batch Gradient Descent优化方法求解的Logistic Regression算法，其算法逻辑如下

![](../img/LR_gd.png)  

其说明如下：

* Learning Rate在迭代过程中衰减:![](../img/LR_lr_ecay.gif) 

	* α为衰减系数
	* T为迭代次数

  
* 模型格式支持稠密和稀疏

	> 目前支持`DoubleDense，DoubleSparse，DoubleSparseLongKey`三种格式，配置参数为 “ml.lr.model.type”

	* **DoubleDense**
		* 参数：T_DOUBLE_DENSE      
		* 特点：适合特征比较稠密的数据。模型用数组存储，节省存储空间，访问速度快，性能高

	* **DoubleSparse**
		* 参数：T_DOUBLE_SPARSE
		* 特点：适合特征稀疏度比较高的数据。模型用Map存储，K为特征ID，V为特征对应的值，K的范围为Int型值域

	* **DoubleSparseLongKey**
		* 参数：T_DOUBLE_SPARSE_LONGKEY
		* 特点：Key可以到Long范围，适合特征稀疏度很高的数据。模型用Map存储，K为特征ID，V为对应的值，K的类型为Long型值域


## 3. 运行 & 性能

### 输入格式
* ml.feature.num：特征向量的维度   
* ml.data.type：支持"dummy"、"libsvm"两种数据格式，具体参考：[Angel数据格式](data_format.md)

###  参数
* 算法参数  
	* ml.epoch.num：迭代次数   
	* ml.batch.sample.ratio：每次迭代的样本采样率   
	* ml.sgd.batch.num：每次迭代的mini-batch的个数   
	* ml.validate.ratio：每次validation的样本比率，设为0时不做validation    
	* ml.learn.rate：初始学习速率   
	* ml.learn.decay：学习速率衰减系数   
	* ml.reg.l2：L2惩罚项系数
	* ml.lr.use.intercept：使用截距   

* 输入输出参数
	* angel.train.data.path：训练数据的输入路径
	* angel.predict.data.path：预测数据的输入路径
	* angel.save.model.path：训练完成后，模型的保存路径
	* angel.predict.out.path：预测结果存储路径
	* angel.log.path：log文件保存路径   

* 资源参数
	 * angel.workergroup.number：Worker个数   
	 * angel.worker.memory.mb：Worker申请内存大小    
	 * angel.worker.task.number：每个Worker上的task的个数，默认为1    
	 * angel.ps.number：PS个数    
	 * angel.ps.memory.mb：PS申请内存大小   


* 提交命令
你可以通过下面命令向Yarn集群提交LR算法训练任务:

	```java
	./bin/angel-submit \
	    --action.type train \
	    --angel.app.submit.class com.tencent.angel.ml.classification.lr.LRRunner  \
	    --angel.train.data.path $input_path \
	    --angel.save.model.path $model_path \
	    --angel.log.path $logpath \
	    --ml.epoch.num 10 \
	    --ml.batch.num 10 \
	    --ml.feature.num 10000 \
	    --ml.validate.ratio 0.1 \
	    --ml.data.type dummy \
	    --ml.learn.rate 1 \
	    --ml.learn.decay 0.1 \
	    --ml.reg.l2 0 \
	    --angel.workergroup.number 3 \
	    --angel.worker.task.number 3 \
	    --angel.ps.number 1 \
	    --angel.ps.memory.mb 5000 \
	    --angel.job.name=angel_lr_smalldata
	```

### 性能
* 数据：视频推荐数据，5×10^7 特征，8×10^7 样本
* 资源：
	* Spark：executor：50个，14G内存，4个core；driver：55G内存
	* Angel：executor：50个，10G内存，4个task；ps：20个，5G内存
* 迭代100次时间：
	* Angel：20min
	* Spark：145min
