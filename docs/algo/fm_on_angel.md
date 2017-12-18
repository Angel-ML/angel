# Factorization Machines      
> 因子分解机(Factorization Machine, FM)是由Steffen Rendle提出的一种基于矩阵分解的机器学习算法。它可对任意的实值向量进行预测。其主要优点包括: 1) 可用于高度稀疏数据场景；2) 具有线性的计算复杂度。

## 1. 算法介绍
### Factorization Model     
* Factorization Machine Model
```math
\hat {y}(x):=w_0+\sum _{i=1} ^n {w_ix_i}+\sum_{i=1}^n\sum_{j=i+1}^n<v_i, v_j>x_ix_j
```
其中：<v_i, v_j>是两个k维向量的点乘：
```math
<v_i, v_j>:=\sum_{i=1}^kv_{i,f} \cdot v_{j,f}
```
模型参数为：
```math
w_0 \in R, w \in R^n, V \in R^{n \times k}
```
V中的v_i表示用k个因子表示特征i，k是决定因子分解的超参数。

### Factorization Machines as Predictors
FM可以被用于一系列的预测任务，比如说：
* 分类：$\hat y$可以直接被用作预测值，优化准则为最小化最小平方差。
* 回归：可以用$\hat y$的符号做分类预测，参数通过合页损失函数或者逻辑回归随时函数估计。

## 2. FM on Angel
* FM算法模型    
FM算法的模型是三个存储在PS上的矩阵，矩阵元信息分别如下：        
    * w_0：偏置项，1×1 维的矩阵。
    * w_1：线性项，1×N 维的矩阵，N为特征个数。
    * v: 因子项，N×k 维的矩阵，N为特征个数，k为因子个数，即每个特征抽象的向量的维度。

* FM训练过程        
    Angel实现了用梯度下降方法优化，迭代得训练FM模型，每次迭代worker和PS上的逻辑如下：       
    * worker：每次迭代从PS上拉取w_0, w_1, v三个矩阵到本地，计算出对应的梯度更新值，push到PS。     
    * PS：PS汇总所有worker推送的模型更新值，取平均，更新PS模型。

## 3. 运行和性能
* 数据格式
    FM算法的训练数据格式为libsvm，其中第一个数值为真实值，后面的数值为“特征ID:特征值”
    ```
    3 1:1 214:1 233:1 234:1 
    ```
* 参数说明            
  * ml.feature.num $featureNum：数据特征个数          
  * ml.epoch.num：训练迭代次数            
  * ml.learn.rate：学习速率          
  * ml.fm.learn.type：学习类型          
  * ml.fm.rank $rank：秩          
  * ml.fm.reg0 $reg0：常数项正则化系数            
  * ml.fm.reg1 $reg1：线性项正则化系数            
  * ml.fm.reg2 $reg2：因子项正则化系数      
  
* 提交命令
    可以通过下面的命令提交FM算法：
```java
../../bin/angel-submit \
--action.type train \
--angel.app.submit.class com.tencent.angel.ml.factorizationmachines.FMRunner  \
--angel.train.data.path $input_path \
--angel.save.model.path $model_path \
--ml.feature.num $featureNum \
--ml.epoch.num $epochNum \
--ml.learn.rate $learnRate \
--ml.fm.learn.type $learnType \
--ml.fm.rank $rank \
--ml.fm.reg0 $reg0 \
--ml.fm.reg1 $reg1 \
--ml.fm.reg2 $reg2 \
--angel.workergroup.number $workerNumber \
--angel.worker.memory.mb $workerMemory  \
--angel.worker.task.number $taskNumber \
--angel.task.data.storage.level $storageLevel \
--angel.task.memorystorage.max.mb $taskMemory \
--angel.ps.number $PSNumber \
--angel.ps.memory.mb $PSMemory \
```
