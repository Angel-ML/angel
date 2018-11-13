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

* **PSModel**
* PSModel is the general name of PSVector/PSMatrix on PS server, including PSClient object
* PSModel is the parent class of PSVector and PSMatrix

* **PSVector**
* PSVector application: Applying PSVector via `PSVector.dense(dim: Int, capacity: Int = 50, rowType:RowType.T_DENSE_DOUBLE) will create a dimension of `dim` with a capacity of `capacity` and a type of `Double `VectorPool, two PSVectors in the same VectorPool can do the operation.
Apply a PSVector with the same VectorPool as `psVector` via `PSVector.duplicate(psVector)`.

* **PSMatrix**
* PSMatrix creation and destruction: created by `PSMatrix.dense(rows: Int, cols: Int)`, after PSMatrix is ​​no longer used, you need to manually call `destory` to destroy the Matrix.

The simple code to use Spark on Angel is as follows:

```Scala

PSContext.getOrCreate(spark.sparkContext)
val psVector = PSVector.dense(dim, capacity)
rdd.map { case (label , feature) =>
    psVector.increment(feature)
    ...
}
println("feature sum:" + psVector.pull.mkString(" "))
```


## 3. Execution Process

Spark on Angel is essentially a Spark application. When Spark is started, the driver starts up Angel PS using Angel PS interface, and when necessary, encapsulates part of the data into PSVector to be managed by PS node. Therefore, the execution process of Spark on Angel is similar to that of Spark. 

**Spark Driver's new execution process:**

Driver has an added action of starting up and managing PS node:

- starting up SparkSession
- starting up PSContext
- creating PSVector/PSMatrix
- executing the logic 
- stopping PSContext and SparkSession

**Spark executor's new execution process:**

