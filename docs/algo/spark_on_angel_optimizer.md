# Spark on Angel Optimizer

## 1. Overview
数据、特征和数值优化算法是机器学习的核心，大家耳熟能详的的数值优化算法包括梯度下降、牛顿法，以及二者的各种变种。
Spark mllib里的算法优化过程，大多是通过调用Breeze库来实现的。
[Breeze](https://github.com/scalanlp/breeze)是一个基于Scala实现的数值运算库，
它提供了丰富的优化算法，同时接口简单易用。

为了使Spark on Angel能支持Breeze里的算法，项目采用了一种很巧妙的实现方式，这就是：**透明替换**。
Breeze中的算法支持Breeze的DenseVector和SparseVector（简称BreezeVector），BreezeVector混入了NumericOps等trait；
例如，L-BFGS算法用到了BreezeVector的dot、scal等操作。

我们在PSVector中也实现了NumericOps等trait，因此我们可以“以假乱真”地让Breeze执行Spark on Angel中PSVector的算法逻辑。
而PSVector之间的运算却发生在Angel PS上，达到无缝地将Breeze的SGD、L-BFGS等算法执行过程透明化的迁移到Angel上。

## 2. Logistic Regression
Logistic Regression是一个非常常见的机器学习分类算法；Logistic Regression可以用不同的优化算法求解，如SGD、L-BFGS、OWL-QN，
然而不同的优化算法在分布式系统中的迭代过程是类似的：基于训练数据计算梯度，然后用梯度更新线性模型的权重。
Spark中Executor分布式计算数据的梯度和loss，通过treeAggregate的方式汇总到Driver，在Driver用梯度更新模型。
Spark on Angel同样是Executor分布式计算数据的梯度和loss，每个Executor分别将梯度值increment到Angel PS上，在Angel PS上梯度更新模型。


Spark mllib中Logistic Regression算法做了很多数据预处理的逻辑，同时其适合于低纬度稠密型的特征；
因此，我们实现了基于高维度、Sparse数据的Logistic Regression，并同时支持SGD、L-BFGS、OWL-QN三种优化方法。

## 3. Spark VS. Spark on Angel
基于以上实现的三种不同优化算法的LR，我们分别在Spark和Spark on Angel上做了实验对比。
- 数据集：腾讯内部某业务的一份数据集，2.3亿样本，5千万维度
- 实验设置：
  + 说明1：三组对比实验的资源配置如下，我们尽可能保证所有任务在资源充足的情况下执行，因此配置的资源比实际需要的偏多
  + 说明2：要加大Spark的spark.driver.maxResultSize参数


|  SGD LR   |     Executor                     |  Driver   |  Angel PS   |
|---|---|---|---|
| Spark | 100 Executors(14G RAM, 2 core)   | 50G        | -   |
| Spark on Angel  |  100 Executors(14G RAM, 2 core)  | 5G | 20 PS(5G RAM, 2core)  |

|  L-BFGS LR   |     Executor                     |  Driver   |  Angel PS   |
|---|---|---|---|
| Spark | 100 Executors(14G RAM, 2 core)   | 50G        | -   |
| Spark on Angel  |  100 Executors(14G RAM, 2 core)  | 5G | 20 PS(10G RAM, 2core)  |

|  OWL-QN LR   |     Executor                     |  Driver   |  Angel PS   |
|---|---|---|---|
| Spark | 100 Executors(14G RAM, 2 core)   | 50G        | -   |
| Spark on Angel  |  100 Executors(14G RAM, 2 core)  | 5G | 50 PS(10G RAM, 2core)  |

- 实验结果对比

| item   |  Spark  |  Spark on Angel  |   加速比例   |
|---|---|---|---|
|SGD LR (step_size=0.05,maxIter=100) | 2.9 hour   | 2.1 hour   | 27.6%  |
|L-BFGS LR (m=10, maxIter=50)        | 2 hour     | 1.3 hour   | 35.0%  |
|OWL-QN LR (m=10, maxIter=50)        | 3.3 hour   | 1.9 hour   | 42.4%  |

如上数据所示，Spark on Angel相较于Spark在训练LR模型时有不同程度的加速；对于越复杂的模型，其加速的比例越大。
同时值得强调的是，Spark on Angel的算法逻辑实现与纯Spark的实现没有太多的差别，大大方便了广大Spark用户。
