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

* Angel MLLib提供了用mini-batch gradient descent优化方法求解的Logistic Regression算法，算法逻辑如下

![](../img/LR_gd.png)  


* 学习速率在迭代过程中衰减:
![](../img/LR_lr_ecay.gif) 其中:   
  * α为衰减系数
  * T为迭代次数




## 3. 运行 & 性能

### 输入格式

数据的格式通过“ml.data.type”参数设置；数据特征的个数，即特征向量的维度通过参数“ml.feature.num”设置。

LR on Angel支持“libsvm”、“dummy”两种数据格式，分别如下所示：

* **dummy格式**

每行文本表示一个样本，每个样本的格式为"y index1 index2 index3 ..."。其中：index特征的ID；训练数据的y为样本的类别，可以取1、-1两个值；预测数据的y为样本的ID值。比如，属于正类的样本[2.0, 3.1, 0.0, 0.0, -1, 2.2]的文本表示为“1 0 1 4 5”，其中“1”为类别，“0 1 4 5”表示特征向量的第0、1、4、5个维度的值不为0。同理，属于负类的样本[2.0, 0.0, 0.1, 0.0, 0.0, 0.0]被表示为“-1 0 2”。

 * **libsvm格式**

每行文本表示一个样本，每个样本的格式为"y index1:value1 index2:value1 index3:value3 ..."。其中：index为特征的ID,value为对应的特征值；训练数据的y为样本的类别，可以取1、-1两个值；预测数据的y为样本的ID值。比如，属于正类的样本[2.0, 3.1, 0.0, 0.0, -1, 2.2]的文本表示为“1 0:2.0 1:3.1 4:-1 5:2.2”，其中“1”为类别，"0:2.0"表示第0个特征的值为2.0。同理，属于负类的样本[2.0, 0.0, 0.1, 0.0, 0.0, 0.0]被表示为“-1 0:2.0 2：0.1”。

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
  * ml.feature.num：数据特征个数   
  * ml.data.type：数据格式，支持"dummy"、"libsvm"    
  * angel.save.model.path：训练完成后，模型的保存路径
  *	angel.predict.out.path：预测结果存储路径
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
* 数据：视频推荐数据，5×10^7特征，8×10^7样本
* 资源：
	* Spark：executor：50个，14G内存，4个core；driver：55G内存
	* Angel：executor：50个，10G内存，4个task；ps：20个，5G内存
* 迭代100次时间：
	* Angel：20min
	* Spark：145min
