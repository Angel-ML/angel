# ADMM_LR

> ADMM[1]是在LogisticRegression中用于求解的一种优化方法。相比于SGD，ADMM可以在少数迭代轮数时就达到一个合理的精度，但是收敛到很精确的解则需要很多次迭代。因此通常ADMM算法会跟其他算法组合来提高收敛速度。

## 1. 算法介绍


## 2. 分布式实现 on Angel

![](../img/admm_lr_1.png)



## 3. 运行 & 性能

###  参数说明

* **算法参数**  
  * ml.epoch.num：迭代次数   
  * ml.reg.l1: L1惩罚系数
  * rho: rho
  * ml.worker.thread.num: 子模型训练并行度

* **输入输出参数**
  * angel.train.data.path：输入数据路径   
  * angel.save.model.path：训练完成后，模型的保存路径   
  * angel.log.path：log文件保存路径   

* **输入格式**
  * ml.feature.num：特征向量的维度   
  * ml.data.type：支持"dummy"、"libsvm"两种数据格式，具体参考:[Angel数据格式](data_format.md)
       
* **资源参数**
  * angel.workergroup.number：Worker个数   
  * angel.worker.memory.mb：Worker申请内存大小    
  * angel.worker.task.number：每个Worker上的task的个数，默认为1    
  * angel.ps.number：PS个数    
  * angel.ps.memory.mb：PS申请内存大小

###  提交命令

可以通过下面命令向Yarn集群提交LR算法训练任务:

```shell
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


* 训练数据

	| 数据集 | 数据集大小 | 数据数量 | 特征数量 | 任务 |
	|:------:|:----------:|:--------:|:--------:|:-------:|
	| XXX  |    350GB    |   1亿  |   5千万   | 二分类 |


* **参数配置**

    * Spark:  200 executor, 20G 内存， Driver 20G内存
    * Angel：100 worker, 10G 内存； 50 PS, 5G 内存
    
* **实验结果**

    ![](../img/admm_lr.png)


## Reference
1. Boyd S, Parikh N, Chu E, et al. [Distributed optimization and statistical learning via the alternating direction method of multipliers](https://pdfs.semanticscholar.org/905b/cb57493c8b97b216bc6786aa122e1ad608b0.pdf). Foundations and Trends® in Machine Learning, 2011, 3(1): 1-122.