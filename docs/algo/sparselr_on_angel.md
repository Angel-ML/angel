# SparseLogisticRegression

SparseLogisticRegression与LogisticRegression的区别在于SparseLogisticRegression会产生稀疏的模型解，使用L1正则项；而LogisticRegression产生的模型是稠密的，使用的是L2正则项。

## ADMM

在Angel中，我们使用ADMM算法[1]来进行LogisticRegression的求解。相比于SGD算法，ADMM算法可以在少数迭代轮数时就达到一个合理的精度，但是收敛到很精确的解则需要很多次迭代。因此通常ADMM算法会跟其他算法组合来提高收敛速度。在Angel中，我们使用breeze库的LBFGS算法在每轮迭代中对子模型进行优化。



## 运行

### 输入格式

数据的格式通过“ml.data.type”参数设置；数据特征的个数，即特征向量的维度通过参数“ml.feature.num”设置。

SparseLR on Angel支持“libsvm”、“dummy”两种数据格式，分别如下所示：

* **dummy格式**

每行文本表示一个样本，每个样本的格式为"y index1 index2 index3 ..."。其中：index特征的ID；训练数据的y为样本的类别，可以取0、1两个值；
 
 * **libsvm格式**

每行文本表示一个样本，每个样本的格式为"y index1:value1 index2:value1 index3:value3 ..."。其中：index为特征的ID,value为对应的特征值；训练数据的y为样本的类别，可以取0、1两个值

###  参数
* 算法参数  
  * ml.epoch.num：迭代次数   
  * ml.reg.l1: L1惩罚系数
  * rho: rho
  * ml.worker.thread.num: 子模型训练并行度

* 输入输出参数
  * angel.train.data.path：输入数据路径   
  * ml.feature.num：数据特征个数   
  * ml.data.type：数据格式，支持"dummy"、"libsvm"    
  * angel.save.model.path：训练完成后，模型的保存路径   
  * angel.log.path：log文件保存路径   
   
* 资源参数
  * angel.workergroup.number：Worker个数   
  * angel.worker.memory.mb：Worker申请内存大小    
  * angel.worker.task.number：每个Worker上的task的个数，默认为1    
  * angel.ps.number：PS个数    
  * angel.ps.memory.mb：PS申请内存大小

## 性能
评测使用腾讯的内部的数据集来比较Angel和Spark的性能。

* 训练数据

| 数据集 | 数据集大小 | 数据数量 | 特征数量 | 任务 |
|:------:|:----------:|:--------:|:--------:|:-------:|
| XXX  |    350GB    |   1亿  |   5千万   | 二分类 |

* **实验环境**

	实验所使用的集群是腾讯的线上Gaia集群(Yarn)，单台机器的配置是：

	* CPU: 2680 * 2
	* 内存：256 GB
	* 网络：10G * 2
	* 磁盘：4T * 12 (SATA)


* **参数配置**

    * Spark: 200 executor, 20G 内存， Driver 20G内存
    * Angel：100 worker, 10G 内存； 50 ps, 5G 内存
    
* **实验结果**

    ![](../img/admm_lr.png)


## Reference
1. Boyd S, Parikh N, Chu E, et al. [Distributed optimization and statistical learning via the alternating direction method of multipliers](https://pdfs.semanticscholar.org/905b/cb57493c8b97b216bc6786aa122e1ad608b0.pdf). Foundations and Trends® in Machine Learning, 2011, 3(1): 1-122.