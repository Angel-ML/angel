# SoftMax Regression      
> Softmax回归也称为多元逻辑回归，既通过逻辑回归算法处理多分类问题。不同于二元逻辑回归，softmax可归可处理K个类别变量的分类问题。

## 1. 算法介绍
### SoftMax Regression     
* SoftMax Regression Model

在逻辑回归中，给出测试输入x，希望假设可以针对同一样本在不同的k（其中，k=1,...,K）值下估计概率 P(y=k|x)的值。假设将会输出K维向量（该向量元素值和为1），结果给出K个类别对应的估计概率值。假设概率估计函数表达如下：
![](../img/SoftMax_p.png)

其中，![](../img/SoftMax_exp.png)

因此，对应的损失函数表达式为：
![](../img/SoftMax_loss.png)

对于上述表达式，我们也通常通过迭代方式优化求解。这里给出迭代过程的梯度表达式：
![](../img/SoftMax_grad.png)

## 2. SoftMax on Angel
* softmax算法模型
softmax算法仅由单个输入层组成，该输入层可为“dense”或“sparse”，同逻辑回归算法，softmax模型的输出只有一个，即分类结果。不同与逻辑回归的是，softmax的损失函数采用了softmax loss。

* softmax regression训练过程
    Angel实现了用梯度下降方法优化，迭代训练得到softmax模型，每次迭代worker和PS上的逻辑如下：       
    * worker：每次迭代从PS上拉取矩阵对应的权重向量到本地，计算出对应的梯度更新值，push到PS对应的梯度向量上。注意，此处的会根据worker端每个样本对应的特征索引拉取对应的模型权重，这样做可减小通信成本同时节约内存以提高计算效率
    * PS：PS汇总所有worker推送的梯度更新值，取平均，通过优化器计算新的模型权重并进行相应更新
    
* softmax预测结果
    * 格式：rowID,pred,prob,label
    * 说明：rowID表示样本所在的行ID，从0开始计数；pred:样本的预测结果值；prob:样本相对该预测结果的概率；label:预测样本被分到的类别，样本根据属于各类别的概率被划分到概率值最大所属的类别，类别标签从0开始计数
    
## 3. 运行和性能
### 数据格式
支持"dense"、"libsvm"和"dummy"三种数据格式，其中"libsvm"格式举例如下:

 ```
 1 1:1 214:1 233:1 234:1
 ```   

 dummy格式如下：
    
 ```
 1 1 214 233 234
 ```

### 参数
* 参数说明           
    * ml.epoch.num：迭代轮数
    * ml.feature.index.range:特征索引范围
    * ml.feature.num：特征维数
    * ml.data.validate.ratio：验证集采样率
    * ml.data.type：数据类型，分“libsvm”和“dummy”两种
    * ml.learn.rate：学习率
    * ml.opt.decay.class.name：学习率衰减系类
    * ml.opt.decay.on.batch: 是否对每个mini batch衰减
    * ml.opt.decay.alpha: 学习率衰减参数alpha
    * ml.opt.decay.beta: 学习率衰减参数beta
    * ml.opt.decay.intervals: 学习率衰减参数intervals
    * ml.reg.l2:l2正则项系数
    * action.type：任务类型，训练用"train",增量训练用"inctrain", 预测用"predict"
    * ml.num.class：输入数据的类别个数
    * ml.inputlayer.optimizer：优化器类型，可选"adam","ftrl"和"momentum"
    * ml.data.label.trans.class: 是否要对标签进行转换, 默认为"NoTrans", 可选项为"ZeroOneTrans"(转为0-1), "PosNegTrans"(转为正负1), "AddOneTrans"(加1), "SubOneTrans"(减1). 
    * ml.data.label.trans.threshold: "ZeroOneTrans"(转为0-1), "PosNegTrans"(转为正负1)这两种转还要以设一个阈值, 大于阈值的为1, 阈值默认为0
    * ml.data.posneg.ratio: 正负样本重采样比例, 对于正负样本相差较大的情况有用(如5倍以上)
 
### 提交命令
* 
    * 可以通过下面的命令提交Softmax算法训练任务：
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

	* 可以通过下面的命令提交Softmax算法增量训练任务：
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

	* 可以通过下面的命令提交Softmax算法预测训练任务：
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
		-Dml.data.label.trans.class="SubOneTrans" \
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

### 性能
* 数据：街景号码图像数据集，3×10^3 特征，7×10^4 样本
* 资源：
	* Angel：executor：10个，10G内存，1个task；ps：4个，10G内存
* 迭代100次时间：
	* Angel：55min
