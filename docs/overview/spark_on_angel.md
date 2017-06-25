# Spark on Angel

Angel在1.0版本开始，就加入了**PS-Service**的特性，不仅仅可以作为一个完整的PS框架运行，也可以作为一个**PS-Service**，为那些不具备参数服务器能力的分布式框架，引入PS能力，让它们能够运行得更快，功能更强。而Spark是这个Service设计的第一个获益者。


作为一个比较流行的内存计算框架，**Spark** 的核心概念是`RDD`，而`RDD`的关键特性之一，是其不可变性，来规避分布式环境下复杂的各种并行问题。这个抽象，在数据分析的领域是没有问题的，它能最大化的解决分布式问题，简化各种算子的复杂度，并提供高性能的分布式数据处理运算能力。

然而在机器学习领域，`RDD`的弱点很快也暴露了。机器学习的核心是**迭代和参数更新**。RDD凭借着逻辑上不落地的内存计算特性，可以很好的解决迭代的问题，然而RDD的不可变性，却非常不适合参数反复多次更新的需求。这本质上的不匹配性，导致了Spark的MLLib库，发展一直非常缓慢，从15年开始就没有实质性的创新，性能也不好，从而给了很多其它产品机会。

现在，基于Angel的平台性设计，Angel可以提供PS-Service，而Spark可以充分利用Angel的参数更新能力，用最小化的修改代价，具备高速训练大模型的能力，写出优雅的机器学习代码，而不必绕来绕去。

## 1. 架构设计

**Spark-On-Angel** 的系统架构如下图所示，可以看到：

1. Spark RDD是不可变区，Angel PS是可变区
2. Angel和Spark之间，都是通过PSAgent和AngelPSClient进行协作和通讯

![](../img/spark_on_angel_architecture.png)

## 2. 核心实现

得益于Angel的接口设计，Spark-On-Angel非常轻量，其主要核心模块包括

* **PSContext**
	* 利用Spark的Context，和Angel的配置，创建AngelContext，在Driver端负责全局的初始化和启动工作

* **PSClient**
	* 负责PSVector与local value直接的运算（包括pull、push、increment）， 以及PSVector与PSVector之间的运算（包括大部分的代数运算）；同时还支持PSF（用户自定义的PS函数）
	* PSClient所有运算会被封装到RemotePSVector和BreezePSVector。

* **PSModelPool**
	* PSModelPool对应了Angel PS上的一个矩阵，PSModelPool负责PSVector的申请、回收、销毁等工作。

* **PSVector/PSVetorProxy**
	* PSVector的RemotePSVector和BreezePSVector封装了在不同场景下的PSVector的运算
		* `RemotePSVector`提供了PSVector与local value直接的运算（包括pull、push、increment）
		* `BreezePSVector`提供了PSVector与PSVector之间的运算（包括大部分的代数运算）
	* PSVectorProxy是PSVector的代理，指向Angel PS上的某个PSVector。



## 3. 运行流程

Spark on Angel的简单代码如下：

```Scala

val psContext ＝ PSContext.getOrCreate(spark.sparkContext)
val pool = psContext.createModelPool(dim, capacity)
val psVector = pool.createModel(0.0)
rdd.map { case (label , feature) =>
  	psVector.increment(feature)
  	...
}
println("feature sum size:" + psVector.mkRemote.size())
```

Spark on Angel本质上是，依然是一个Spark任务。Spark启动后，driver通过Angel PS的接口启动Angel PS，必要时将部分数据封装成PSVector丢给PS node管理。 因此，整个Spark on Angel的执行过程与Spark差别不多。

**Spark Driver的新执行流程为：**

Driver多了负责启动、管理PS node的动作

- 启动SparkSession
- 启动PSContext
- 创建PSModelPool
- 申请PSVector
- 执行算法逻辑
- 终止PSContext和SparkSession

**Spark executor的新执行流程**

通过transformation的函数调用，Executor在需要的时候向PS Server发起对PSVector操作的请求

- 启动PSContext
- 执行Driver分配的task

> 可以看到，这样的运行方式，不需要修改Spark的Core代码

## 4. MLLib无缝切换

Spark的MLLib中，有大量的算法包，为了让它们，能轻松跑在Spark on Angel上，我们采用了一种比较取巧的实现方式，这就是：**透明替换**。

Spark中MLlib算法的核心是Breeze库，所有核心算法，最终都是通过混入了NumericOps特征的BreezeVector来实现的。例如，LBFGS算法用到了BreezeVector的dot、scal等操作。

因此，如果我们实现了一个混入相同特征的PSVector，支持了这些操作，我们就可以无缝的，将调用Breeze的LBFGS算法，将其在BreezeVector上的优化求解过程，透明化的迁移到Angel上，让这些计算发生在Angel之上，而无须对RDD进行任何侵入性修改。

![](../img/spark_on_angel_vector.png)


下面是两个代码示例，展示了原生的Spark，和Spark on Angel的写法不同：

* **Spark版本**

```Scala

def runOWLQN(trainData: RDD[(Vector, Double)], dim: Int, m: Int, maxIter: Int): Unit = {

    val initWeight = new DenseVector[Double](dim)
    val l1reg = 0.0
    val owlqn = new BrzOWLQN[Int, DenseVector[Double]](maxIter, m, 0.0, 1e-5)

    val states = owlqn.iterations(CostFunc(trainData), initWeight)
    ……

}
```

* **Spark on Angel版本**

```Scala

def runOWLQN(trainData: RDD[(Vector, Double)], dim: Int, m: Int, maxIter: Int): Unit = {

    val pool = PSContext.createModelPool(dim, 20)

    val initWeightPS = pool.createZero().mkBreeze()
    val l1regPS =  pool.createZero().mkBreeze()

    val owlqn = new OWLQN(maxIter, m, l1regPS, tol)
    val states = owlqn.iterations(CostFunc(trainData), initWeightPS)
    ………

｝
```

可以看到，代码的改动量非常小，对原生的RDD也没有任何的侵入，对于整体Spark框架的社区融合和升级，都是非常友好。

## 性能

算法从Spark切换到Spark on Angel之上，性能是得到了不错的提升。具体可以参考[LR(Spark on Angel)](../algo/spark_on_angel_optimizer.md)。 

但是需要提醒的是，这样的替换方式，性能肯定不如从头按照Angel的特点，按照PS的特性，实现一次来得性能更高的，但是好处是节省工作量，而且具备通用性。建议如果想实现最高性能的算法，可以尝试自己动手。当然了，直接用Angel实现，性能会是最好的，毕竟不用隔了一层PSAgent。
