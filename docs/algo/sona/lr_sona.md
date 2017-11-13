# [Spark on Angel] LR


> 数据、特征和数值优化算法是机器学习的核心，Spark on Angel的LR算法，借助Spark本身的MLLib，支持更加完善的优化算法，包括梯度下降、牛顿法，以及二者的各种变种。

## 1. 算法介绍

Spark MLLib中Logistic Regression算法，适合于低维度稠密型的特征，并可以用不同的优化算法求解，如SGD、L-BFGS、OWL-QN……不同的优化算法在分布式系统中的迭代过程是类似的，都是基于训练数据计算梯度，然后用梯度更新线性模型的权重。

为了最大程度复用Spark MLlib的工作和各种优化算法，Spark on Angel的LR算法，对Spark的LR做了轻微的改动，可以适用于高维度的稀疏模型，而且可以复用原来的各种优化算法。

## 2. 分布式实现 with Spark on Angel

* **整体实现对比**

	* Spark中Executor分布式计算数据的梯度和loss，通过treeAggregate的方式汇总到Driver，在Driver用梯度更新模型。

	* Spark on Angel同样是Executor分布式计算数据的梯度和loss，每个Executor分别将梯度值increment到Angel PS上，在Angel PS上梯度更新模型。

* **优化算法迁移**

	Spark MLLib里的算法优化过程，大多是通过调用Breeze库来实现的。
	[Breeze](https://github.com/scalanlp/breeze)是一个基于Scala实现的数值运算库，
	它提供了丰富的优化算法，同时接口简单易用。

	为了使Spark on Angel能支持Breeze里的算法，项目采用了一种比较巧妙的实现方式，这就是：[**透明替换**]()。无缝地将Breeze的SGD、L-BFGS、OWL-QN三种优化方法透明化的迁移到Angel上。

## 3. 运行 & 性能

* **参数**

* **命令**

* **性能**
	- 数据集：腾讯内部某业务的一份数据集，2.3亿样本，5千万维度

	- 实验设置
	  1. 三组对比实验的资源配置如下，我们尽可能保证所有任务在资源充足的情况下执行，因此配置的资源比实际需要的偏多
	  2. 要加大Spark的spark.driver.maxResultSize参数

	- 结果

		| item   |  Spark  |  Spark on Angel  |   加速比例   |
		|---|---|---|---|
		|SGD LR (step_size=0.05,maxIter=100) | 2.9 hour   | 1.5 hour   | 48.3%  |
		|L-BFGS LR (m=10, maxIter=50)        | 2 hour     | 1.0 hour   | 50.0%  |
		|OWL-QN LR (m=10, maxIter=50)        | 3.3 hour   | 1.4 hour   | 57.6%  |

* **资源**

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

