# Spark on Angel

The PS-Service feature was introduced in Angel 1.0.0. It can not only run as a complete PS framework, but also a **PS-Service** that adds the PS capability to distributed frameworks to make them run faster with more powerful features. Spark is the first beneficiary of the PS-Service design. 

As a popular in-memory computing framework, **Spark** revolves around the concept of `RDD`, which is *immutable* to avoid a range of potential problems due to updates from multiple threads at once. The RDD abstraction works just fine for data analytics because it solves the distributed problem with maximum capacity, reduces the complexity of various operators, and provides high-performance, distributed data processing capabilities. 

In machine learning domain, however, **iteration and parameter updating** is the core demand. `RDD` is a lightweight solution for iterative algorithms since it keeps data in memory without I/O; however, `RDD`'s immutability is a barrier for repetitive parameter updates. We believe this tradeoff in RDD's capability is one of the causes of the slow development of Spark MLLib, which lacks substantive innovations and seems to suffer from unsatisfying performance in recent years. 

Now, based on its platform design, Angel provides PS-Service to Spark. Spark can  take full advantage of parameter updating capabilities. Complex models can be trained efficiently in elegant code with minimal cost of rewriting.     

## 1. Architecture Design 

**Spark-On-Angel**'s system architecture is shown below. Notice that

* Spark RDD is the immutable layer, whereas Angel PS is the mutable layer
* Angel and Spark cooperate and communicate via PSAgent and AngelPSClient

![](../img/spark_on_angel_architecture.png)

## 2. Core Implementation

Spark-On-Angel is lightweight due to Angel's interface design. The core modules include:

* **PSContext**
	* uses Spark context and Angel configuration to create PSContext, in charge of overall initializing and starting up on the driver side

* **PSClient**
	* responsible for direct operations between PSVector and local value, including pull, push, and increment, as well as operations between PSVector and PSVector, including most algebraic operations; supporting PSF ( user-defined PS functions）
	* all PSClient operations are encapsulated into RemotePSVector and BreezePSVector

* **PSModelPool**
	* PSModelPool corresponds to a matrix on Angel PS, responsible for requesting, retrieving, and destructing PSVector 

* **PSVector/PSVetorProxy**
	* RemotePSVector and BreezePSVector are encapsulated with PSVector's operations under different scenarios
		* `RemotePSVector` provides operations between PSVector and local value, including pull, push, increment
		* `BreezePSVector` provides operations between PSVector and PSVector, including most algebraic operations
	* PSVectorProxy is PSVector's proxy that points to a PSVector on Angel PS
	
* **PSMatrix**
	* Including DensePSMatrix and SparsePSMatrix
	* Construction and destruction of PSMatrix: Use ```PSMatrix.dense(rows: Int, cols: Int)``` to construct a PSMatrix. When the Matrix is not needed, call ```destroy``` to destruct it manually


## 3. Execution Process

A sample code of Spark on Angel looks like this:

```Scala

val psContext = PSContext.getOrCreate(spark.sparkContext)
val pool = psContext.createModelPool(dim, capacity)
val psVector = pool.createModel(0.0)
rdd.map { case (label , feature) =>
  	psVector.increment(feature)
  	...
}
println("feature sum size:" + psVector.mkRemote.size())
```

Spark on Angel is essentially a Spark application. When Spark is started, the driver starts up Angel PS using Angel PS interface, and when necessary, encapsulates part of the data into PSVector to be managed by PS node. Therefore, the execution process of Spark on Angel is similar to that of Spark. 

**Spark Driver's new execution process:**

Driver has an added action of starting up and managing PS node:

- starting up SparkSession
- starting up PSContext
- creating PSModelPool
- requesting PSVector
- executing the logic 
- stopping PSContext and SparkSession

**Spark executor's new execution process:**

Spark executor sends request of operations of PSVector to PS Server, when needed, by calling the transformation method 

- starting up PSContext
- executing tasks assigned by the driver

> It's worth noting that there is no need to modify Spark's any core source code in this process

## 4. Seamless Switch to MLLib

In order for the algorithms in Spark MLLib to run in Spark on Angel efficiently, we use a trick which is called **transparent replacement**.

Breeze is the core library for numerical processing for Scala. Many data structures of MLLib are modeled around breeze data structures, and core algorithms in MLLib are implemented as operations on BreezeVectors defined in NumericOps trait, an example being LBFGS's usage of BreezeVector's operations, such as dot, scal, among others.

Based on the above fact, if we implement a PSVector that incorporates the same traits and supports the same operations, we can then transfer the operations on BreezeVector to happen on PSVector, thus making MLLib algorithms run on Angel seamlessly.   

![](../img/spark_on_angel_vector.png)


Let's review the difference between sample code for Spark and Spark on Angel

* **Spark**

```Scala

def runOWLQN(trainData: RDD[(Vector, Double)], dim: Int, m: Int, maxIter: Int): Unit = {

    val initWeight = new DenseVector[Double](dim)
    val l1reg = 0.0
    val owlqn = new BrzOWLQN[Int, DenseVector[Double]](maxIter, m, 0.0, 1e-5)

    val states = owlqn.iterations(CostFunc(trainData), initWeight)
    ……

}
```

* **Spark on Angel**

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

There is only a small, non-invasive modification to the original RDD, friendly to the overall Spark framework and other integration and upgrading.

## Performance

Transferring algorithms from Spark to Spark on Angel results in a noticeable gain in performance, and details can be found in [LR(Spark on Angel)](../algo/spark_on_angel_optimizer_en.md). 

It is worth noting that even though the transparent replacement trick is versatile and incurs only a small workload, the best performance is still only achievable by implementing the algorithm on top of PS that is specific to Angel (at least, you get rid of the PSAgent layer). 
