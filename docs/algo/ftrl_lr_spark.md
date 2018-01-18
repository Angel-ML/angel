# [Spark Streaming on Angel] FTRL

>FTRL是一种在线学习的常见优化算法，方便实用，而且效果很好，常用于更新在线的CTR预估模型。之前的传统实现多见于Storm。但是实际应用中，数据集的维度往往很大，且模型稀疏性，这种Case下，用Spark Streaming结合Angel，其实会有更好的效果，而且代码很少，性能稳健。


## 1. 算法介绍

 `FTRL`算法兼顾了`FOBOS`和`RDA`两种算法的优势，既能同FOBOS保证比较高的精度，又能在损失一定精度的情况下产生更好的稀疏性。
 
该算法的特征权重的更新公式为：

![](../img/ftrl_lr_w.png)

其中

* G函数表示损失函数的梯度

![](../img/ftrl_lr_g.png)

* w的更新公式（针对特征权重的各个维度将其拆解成N个独立的标量最小化问题）

![](../img/ftrl_lr_w_update.png)

* 如果对每一维度的学习率单独考虑，w的更新公式：

![](../img/ftrl_lr_d_t.png)


## 2.分布式实现

Google给出的带有L1和L2正则项的基于FTRL优化的逻辑回归算法的工程实现

![](../img/ftrl_lr_project.png)

为了加快收敛速度，算法还提供了基于SVRG的方差约减FTRL算法，即在梯度更新时对梯度进行方差约减。

* SVRG的一般过程为：

![](../img/svrg.png)

为此，算法在损失函数的梯度g处增加了一步基于SVRG的更新，同时为了符合SVRG算法的原理，增加了两个参数rho1，rho2,近似计算每个阶段的权重和梯度。给出基于SVRG算法的FTRL算法（后文简称"FTRL_VRG"）的一般过程：

![](../img/ftrl_vrg.png)

参考实现，结合Spark Streaming和Angel的特点，FTRL的分布式实现的框架图如下：

![](../img/ftrl_lr_framework.png)

FTRL_VRG的分布式实现框架图如下：

![](../img/ftrl_vrg_framework.png)

## 3. 运行 & 性能



###  **输入格式**
* dim：输入数据的维度,默认从0开始计数 
* 格式：消息格式仅支持标准的["libsvm"](./data_format.md)数据格式
* 本算法以kafka为消息发送机制，使用时需要填写kafka的配置信息。

### **参数说明**

* **算法参数**  
	* alpha：w更新公式中的alpha 
	* beta: w更新公式中的beta
	* lambda1: w更新公式中的lambda1
	* lambda2: w更新公式中的lambda2
	* rho1:FTRL_VRG中的权重更新系数
	* rho2:FTRL_VRG中的梯度更新系数

* **输入输出参数**
	 * checkPointPath：streaming流数据的checkpoint路径   
	 * actionType："train"表示训练，"predict"表示预测
	 * sampleRate：预测时用来控制样本的输入比例
	 * zkQuorum:Zookeeper的配置信息，格式："hostname:port"
	 * topic:kafka的topic信息
	 * group:kafka的group信息
	 * receiverNum:kafka receiver的个数
	 * streamingWindow：控制spark streaming流中每批数据的持续时间
	 * modelSavePath：训练时模型的保存路径，预测时模型的加载路径
	 * logPath:每个batch的平均loss输出路径
	 * partitionNum：streaming中的分区数
	 * optMethod:选择采用ftrl还是ftrlVRG进行优化
	 * isIncrementLearn:是否增量学习
	 * batch2Check:间隔多少个batch对训练集的loss进行一次计算
	 * batch2Save:间隔多少个batch对模型进行一次保存
	 * input:预测时是输入路径
	 * output:预测结果的输出路径

* **资源参数**
	* num-executors：executor个数   
	* executor-cores：executor的核数    
	* executor-memory：executor的内存    
	* driver-memory：driver端内存    
	* spark.ps.instances:Angel PS节点数
	* spark.ps.cores:每个PS节点的Core数
	* spark.ps.memory：每个PS节点的Memory大小

###  **提交命令**

可以通过下面命令向Yarn集群提交FTRL_SparseLR算法的训练任务:

```shell
./bin/spark-submit \
--master yarn-cluster \
    --conf spark.hadoop.angel.ps.ha.replication.number=2 \
    --conf fs.default.name=hdfs://tl-nn-tdw.tencent-distribute.com:54310 \
    --conf spark.yarn.allocation.am.maxMemory=55g \
    --conf spark.yarn.allocation.executor.maxMemory=55g \
    --conf spark.ps.jars=$SONA_ANGEL_JARS \
    --conf spark.ps.instances=20 \
    --conf spark.ps.cores=2 \
    --conf spark.ps.memory=6g \
    --jars $SONA_SPARK_JARS \
    --name $name \
    --driver-memory 5g \
    --num-executors 10 \
    --executor-cores 2 \
    --executor-memory 12g \
    --class com.tencent.angel.spark.ml.online_learning.FTRLRunner \
    spark-on-angel-mllib-2.2.0.jar \
    input:$input \
    output:$output \
    actionType:train \
    sampleRate:0.1 \
    partitionNum:10 \
    modelSavePath:$modelSavePath \
    checkPointPath:$checkPointPath \
    logPath:$logPath \
    group:$group \
    master:$master \
    topic:$topic \
    bid:$bid \
    tid:$tid \
    rho1:0.2 \
    rho2:0.2 \
    alpha:0.1 \
	isIncrementLearn:false \
    lambda1:0.3 \
    lambda2:0.3 \
    dim:175835 \
    streamingWindow:10 \
    receiverNum:10 \
    batch2Check:50 \
    batch2Save:10 \
    optMethod:ftrlVRG


```

