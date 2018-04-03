# LogisticRegression

> 逻辑回归模型（logistic regression model）是一种分类模型。它是最常见和常用的一种分类方法，在传统的广告推荐中被大量使用，朴实但有效。

## 1. 算法介绍

逻辑回归模型（logistic regression model）是一种分类模型。样本x属于类别y的概率P(y|x)服从logistic分布：   

![model](http://latex.codecogs.com/png.latex?\dpi{150}P(y=+1|x)=\frac{1}{1+\exp(-wx)},P(y=-1|x)=\frac{1}{1+\exp(wx)})

综合两种情况，有：      

![model](http://latex.codecogs.com/png.latex?\dpi{150}P(y|x)=\frac{1}{1+\exp(-ywx)})

逻辑回归模型使用log损失函数，带L2惩罚项的目标函数如下所示：    

![model](http://latex.codecogs.com/png.latex?\dpi{150}\min_w\sum_i^N\log(1+\exp(-y_iwx_i))+\lambda\\|w\\|_2^2)

其中：![](http://latex.codecogs.com/png.latex?\dpi{100}\inline%20\lambda\|w\|_2^2)为L2正则项。

## 2. 分布式实现 on Angel

Angel MLLib提供了用Mini-Batch Gradient Descent优化方法求解的Logistic Regression算法，其算法逻辑如下

![](../img/LR_gd.png)  

其说明如下：

* Learning Rate在迭代过程中衰减:

![](http://latex.codecogs.com/png.latex?\dpi{150}\eta=\frac{\eta_0}{\sqrt{1+\alpha\cdot%20T}})

其中, α为衰减系数, T为迭代次数

  
* 模型格式支持稠密和稀疏，32 bit和64bit

	> 目前支持`T_DOUBLE_DENSE，T_DOUBLE_SPARSE，T_DOUBLE_SPARSE_COMPONENT，T_DOUBLE_SPARSE_LONGKEY，T_DOUBLE_SPARSE_LONGKEY_COMPONENT `五种格式，配置参数为 “ml.model.type”

	* **T_DOUBLE_DENSE**
		* 含义：稠密double     
		* 特点：适合特征比较稠密的数据。模型用数组存储，节省存储空间，访问速度快，性能高。该选项为默认配置

	* **T_DOUBLE_SPARSE**
		* 含义：稀疏double，index为int类型 或者 T_DOUBLE_SPARSE_COMPONENT
		* 特点：适合特征稀疏度比较高的数据。模型使用Map存储，K为特征ID，V为特征对应的值，K的范围为Int型值域。T_DOUBLE_SPARSE类型使用一个单一的Map来存储整个模型，适合模型非零值不是很多的场景
			
	* **T_DOUBLE_SPARSE_COMPONENT**
		* 含义：稀疏double，index为int类型
		* 特点：与T_DOUBLE_SPARSE类似，不同的是T_DOUBLE_SPARSE_COMPNENT使用多个较小的Map来表示整个模型，适合非零值较多的场景，在一些计算场景下可以使用多个子Map并行计算的方式加速稀疏型向量的计算， 同时可以降低超大Map给内存带来的压力

	* **T_DOUBLE_SPARSE_LONGKEY**
		* 含义：稀疏double型，index 类型为long
		* 特点：Key可以到long范围，适合特征稀疏度很高的数据。模型用Map存储，K为特征ID，V为对应的值，K的类型为Long型值域。T_DOUBLE_SPARSE_LONGKEY类型使用一个单一的Map来存储整个模型，适合模型非零值不是很多的场景。若选择了这种模型格式，计算时会自动按照64位index来解析训练数据
			
	* **T_DOUBLE_SPARSE_LONGKEY_COMPONENT**
		* 含义：稀疏double型，index 类型为long
		* 特点：与T_DOUBLE_SPARSE_LONGKEY类似，不同的是T_DOUBLE_SPARSE_LONGKEY_COMPNENT使用多个较小的Map来表示整个模型，适合非零值较多的场景，在一些计算场景下可以使用多个子Map并行计算的方式加速稀疏型向量的计算， 同时可以降低超大Map给内存带来的压力


## 3. 运行 & 性能

### 输入格式
* ml.feature.index.range：特征向量的维度, 即特征index的范围：例如如果index范围为[0, 100000000]， 则可以将该参数配置为100000000；这个参数也可以配置为-1，表示index 范围为[Integer.MIN_VALUE, Integer.MAX_VALUE] 或者[Long.MIN_VALUE, Long.MAX_VALUE]
* ml.model.size: 模型大小, 对于一些稀疏模型, 存在一些无效维度, 即所有样本要这一维度上的取值匀为0. ml.model.size = ml.feature.index.range - number of invalidate indices
* ml.data.type：支持"dummy"、"libsvm"两种数据格式，具体参考：[Angel数据格式](data_format.md)

###  参数
* 算法参数  
	* ml.epoch.num：迭代次数   
	* ml.batch.sample.ratio：每次迭代的样本采样率   
	* ml.num.update.per.epoch：一个epoch中更新参数的次数
	* ml.data.validate.ratio：每次validation的样本比率，设为0时不做validation
	* ml.learn.rate：初始学习速率   
	* ml.learn.decay：学习速率衰减系数
	* ml.reg.loss.type：正则项类型，目前可以配置**loss1**和**loss2**，**loss1**表示使用L1正则项，**loss2**表示使用L2正则项
	* ml.lr.reg.l1：L1惩罚项系数，仅当reg.loss.type配置为**loss1**时有效
	* ml.lr.reg.l2：L2惩罚项系数，仅当reg.loss.type配置为**loss2**时有效
	* ml.lr.use.intercept：使用截距 
	* ml.pull.with.index.enable：是否使用基于index的模型获取，**true**表示使用index来获取模型的指定部分，**false**表示不使用，默认为**false**。 当模型稀疏度较高时，建议配置为**true**。当该选择配置为**true**时，在LR的训练数据预处理过程中，算法会自动记录训练数据中出现的特征的index，在获取模型时会根据这些index来获取模型

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
	    --ml.num.update.per.epoch 10 \
	    --ml.ml.feature.index.range 10000 \
	    --ml.data.validate.ratio 0.1 \
	    --ml.data.type dummy \
	    --ml.learn.rate 1 \
	    --ml.learn.decay 0.1 \
	    --ml.lr.reg.l2 0 \
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
