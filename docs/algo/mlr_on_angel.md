# MLR
## 1. 算法介绍
### 模型
MLR是一个分区域的线性模型，广泛应用于广告ctr预估。MLR采用分治策略：首先将特征空间划分为多个局域区间，然后在每个局域区间拟合一个线性模型，输出结果是多个线性模型的加权和，这两步以最小化损失函数为目标，同时学习得到。算法详细参见：Large Scale Piece-wise Linear Model (LS-PLM)。
MLR算法有三个明显的优点：
1. 非线性：选择足够多的划分区域，MLR算法可以拟合任意复杂的非线性函数。
2. 扩展性：与LR算法相似，MLR算法对海量样本，超高维度模型都有很好的扩展性。
3. 稀疏性：带${L_1}，${L_{2,1}}正则项的MLR算法可以得到很好的稀疏性

### 公式        
```math
p(y=1|x)=g( \Sigma_{j=1}^m \sigma(u_j^Tx) \eta (w_j^T x))
```
其中：${\sigma(\cdot)} 是划分函数，${w_1,w_2,...,w_m}是拟合函数${\eta (\cdot)}的参数。对于一个给定样本x，我们的预测函模型${p（y|x)}有两部分，第一部分${\sigma (u_ {j^T} x)}把特征空间划分为m个区域，第二部分${\eta(w_j^Tx)}给出每个区域的预测值。函数${g(\cdot)}确保模型满足概率函数的定义。

MLR算法模型使用softmax作为划分函数${\sigma (x)}，sigmoid函数作为拟合函数${\eta (x)}，并且：${g(x)=x}，得到MLR的模型如下：
```math
p(y=1|x)=\Sigma^m_{i=1} \frac{exp(u_i^Tx)}{\Sigma_{j=1}^m exp(u_j^Tx)} \cdot \frac{1}{1+exp(-w_i^Tx)}   
```

## 2. 分布式实现 on Angel
### 梯度下降法
对于 ${y \in {-1, 1}}，模型可以统一形式：
```math
p(y=1|x)=\Sigma^m_{i=1} \frac{exp(u_i^Tx)}{\Sigma_{j=1}^m exp(u_j^Tx)} \cdot \frac{1}{1+exp(-yw_i^Tx)}

=\Sigma^m_{i=1} \frac{exp(u_i^Tx)}{\Sigma_{j=1}^m exp(u_j^Tx)} \cdot \sigma(yw_i^Tx)
```
对于样本(x, y)，损失函数为：
```math
l(x,y)=-\ln{P(y|x)}=-\ln \frac{1}{\Sigma_{j=1}^m e^{u_j^Tx}} \Sigma_{i=1}^m{e^{u_i^Tx}} \sigma(yw_i^Tx)

= \ln \Sigma_{j=1}^m e^{u_j^Tx} - \ln (\Sigma_{i=1}^m e^{u_i^Tx} \sigma(yw_i^Tx))
```
梯度：
```math
\triangledown_{u_k}l = \frac{e^{u_k^Tx}x}{\Sigma_{j=1}^m e^{u_j^T}x} - \frac{e^{u_k^Tx} \sigma(yw_k^Tx)x}{\Sigma_{i=1}^m e^{u_i^Tx} \sigma(yw_i^Tx)}

\triangledown_{w_k} l = \frac {ye^{u_k^Tx} \sigma(yw_k^Tx)(\sigma(yw_k^Tx)-1)x}{\Sigma_{i=1}^m e^{u_i^Tx} \sigma(yw_i^Tx)}
```

### 基于Angel的实现
* 模型存储：
    * MLR算法的模型参数有：softmax函数参数：${u_1, u_2, ..., u_m}，sigmoid函数参数：${w_1,w_2,...,w_m}     。其中${u_i}、${w_i}为N维向量，N为数据的维度，即特征个数。用两个m*N维的矩阵分别表示softmax矩阵、sigmodi矩阵。
    * 用两个m*1维度的矩阵，分别表示softmax函数、sigmodi函数的截断值。
    
* 模型计算：    
    * 用梯度下降法训练MLR模型，算法以迭代的方式进行。每次迭代开始，worker从PS上拉最新的模型参数，并用自己的训练数据计算出梯度，将梯度推送给PS。
    * PS接收到所有worker推送的梯度值，取平均，更新PSModel。

## 3. 运行
### 输入格式

数据的格式通过“ml.data.type”参数设置；数据特征的个数，即特征向量的维度通过参数“ml.feature.num”设置。

MLR on Angel支持“libsvm”、“dummy”两种数据格式，分别如下所示：

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
  * ml.mlr.rank：区域个数，对应于模型公式中的m
  * ml.mlr.v.init：模型初始化参数，高斯分布的标准差值

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
    --angel.app.submit.class com.tencent.angel.ml.classification.mlr.MLRRunner  \
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
    --ml.mlr.rank 5 \
    --ml.mlr.v.init 0.00000001 \ 
    --angel.workergroup.number 3 \
    --angel.worker.task.number 3 \
    --angel.ps.number 1 \
    --angel.ps.memory.mb 5000 \
    --angel.job.name=angel_lr_smalldata
```


[1]Learning Piece-wise Linear Models from Large Scale Data for Ad Click Prediction
