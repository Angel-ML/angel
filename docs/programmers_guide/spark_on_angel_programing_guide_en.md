# Spark on Angel Programming Guide

Spark on Angel is designed to enable easy migration for Spark development with minimal cost of change. Therefore, the implementation of algorithms in Spark on Angel is very similar to that in Spark. The majority of Spark ML algorithms can run in Spark on Angel with only a small code change.

In its current version，Spark on Angel is developed with Spark 2.1.1 and Scala 2.11.8.


## Importing Spark on Angel

To write a Spark on Angel application, in addition to the Spark dependency, you need to add the following Maven dependencies as well:

```xml
<dependency>
    <groupId>com.tencent.angel</groupId>
    <artifactId>spark-on-angel-core</artifactId>
    <version>${angel.version}</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>com.tencent.angel</groupId>
    <artifactId>spark-on-angel-mllib</artifactId>
    <version>${angel.version}</version>
    <scope>provided</scope>
</dependency>
```

The corresponding import and implicit conversion:

```scala
  import com.tencent.angel.spark._
  import com.tencent.angel.spark.PSContext
```

## Initializing Spark on Angel

Start Spark and initialize SparkSession first, then start PSContext through SparkSession. Set all config parameters for Spark and Angel PS to `builder`, and Angel PS will get the configuration information from SparkConf.

```scala
// Initialize Spark
val builder = SparkSession.builder()
  .master(master)
  .appName(appName)
  .config("spark.ps.num", "x")
  .config("B", "y")
val spark = builder.getOrCreate()

// Initialize Angel's PSContext
val context = PSContext.getOrCreate(spark.sparkContext)
```

In Angel PS, all operations on the driver side are encapsulated into PSContext. The interfaces for initializing and stopping PS Server are similar as SparkSession/sparkContext for Spark.

```scala
// Stop PSContext
PSContext.stop()
```

## PSModelPool

Compared to Angel's PSModel, Spark on Angel's core abstraction, PSModelPool，contains some more subtle adjustments for it to better interact with Spark. It is worth noting that:

> On PSServer, Angel client and Spark on Angel client are treated equally



In Angel PS, PS ModelPool is essentially a matrix with number of rows as `capacity` and number of columns as `dim`. One can create multiple PSModelPool of different sizes in one application. In Spark on Angel, PSModelPool is the counterpart of PSModel for Angel.

* Creating ModelPool

```scala
val pool = context.createModelPool(dim, capacity)
```

* Destroying ModelPool

```scala
context.destroyModelPool(pool)
```


### PSVectorProxy

PSModelPool can create PSVector. A PSVector must have exactly `dim` dimensions in order to be stored and managed by PSModelPool with `dim` columns.

PSVectorProxy is the proxy for PSVector (including BreezePSVector and RemotePSVector), pointing to a PSVector on Angel PS.


```scala
// Create a PSVector that must have the same dimension as the pool
val arrayProxy = pool.createModel(array)
// Create a PSVector whose element on each dimension is value
val valueProxy = pool.createModel(value)
// Create PSVector
val zeroProxy = pool.createZero()
// Create a random PSVector with each element uniformly distributed 
val uniformProxy = pool.createRandomUniform(0.0, 1.0)
// Create a random PS Vector with each element normally distributed
val normalProxy = pool.createRandomNormal(0.0, 1.0)
```

You can either manually delete PSVectorProxy (in this way, the deleted PSVectorProxy can't be reused), or let the system automatically close it.

```scala
pool.delete(vectorProxy)
```

### PSVector

BreezePSVector and RemotePSVector are both subclasses of PSVector that encapsulate PSVector operations for different scenarios.

- **RemotePSVector**
  
RemotePSVector encapsulates operations between PSVector and local Array

```scala
  // Pull PSVector to local
  val localArray = remoteVector.pull()
  // Push local array to Angel PS
  remoteVector.push(localArray)
  // Increment local Array to PSVector on Angel PS
  remoteVector.increment(localArray)

  // Find the maximum/minimum number of each dimension for local array and PSVector
  remoteVector.mergeMax(localArray)
  remoteVector.mergeMin(localArray)
```

- **BreezePSVector**

BreezePSVector encapsulates operations between PSVectors in PSModelPool, including the commonly used math operations and blas operations. BreezePSVector implements the internal NumbericOps for Breeze, thus supports operations such as `+`,  `-`, `*`

```scala
  val brzVector1 = (brzVector2 :* 2) + brzVector3
```

One can also explicitly call Breeze.math and Breeze.blas

- **Conversion**


```scala
  // PSVectorProxy to BreezePSVector, RemotePSVector
  val brzVector = vectorProxy.mkBreeze()
  val remoteVector = vectorProxy.mkRemote()

  // BreezePSVector, RemotePSVector to PSVectorProxy
  val vectorProxy = brzVector.proxy
  val vectorProxy = remoteVector.proxy

  // Conversion between BreezePSVector and RemotePSVector
  val remoteVector = brzVector.toRemote()
  val brzVector = remoteVector.toBreeze()
```

## psFunc

- Spark on Angel supports psFunc just like Angel does, with even more powerful functional-programming features. psFunc inherits interfaces such as MapFunc and MapWithIndex to implement user-defined PSVector operations.

```scala
val result = brzVector.map(func)
val result = brzVector.mapWithIndex(func)
val result = brzVector.zipMap(func)
```
`func` above must inherit MapFunc and MapWithIndexFunc, and implement user-defined logic and serializable interface.


## Sample Code

1. **Updating PSVector**

	Increment all features in RDD[(label, feature)] to PSVector.


	```Scala
	val dim = 10
	val poolCapacity = 40

	val context = PSContext.getOrCreate()
	val pool = context.createModelPool(dim, poolCapacity)
	val psProxy = pool.zero()

	rdd.foreach { case (label , feature) =>
	  psProxy.mkRemote.increment(feature)
		}

	println("feature sum:" + psProxy.pull())
	```

2. **Implementing Gradient Descent**

	Simplest implementation of Gradient Descent in Spark on Angel:

	```Scala
	val context = PSContext.getOrCreate()
	val pool = context.createModelPool(dim, poolCapacity)
	val w = pool.createModel(initWeights)
	val gradient = pool.zeros()

	for (i <- 1 to ITERATIONS) {
	  val totalG = gradient.mkRemote()

	  val nothing = points.mapPartitions { iter =>
	    val brzW = new DenseVector(w.mkRemote.pull())

	    val subG = iter.map { p =>
	      p.x * (1 / (1 + math.exp(-p.y * brzW.dot(p.x))) - 1) * p.y
	    }.reduce(_ + _)

	    totalG.incrementAndFlush(subG.toArray)
	    Iterator.empty
	  }
	  nothing.count()

	  w.mkBreeze += -1.0 * gradent.mkBreeze
	  gradient.mkRemote.fill(0.0)
	}

	println("feature sum:" + w.mkRemote.pull())

	gradient.delete()
	w.delete()
	```
	
3. **More Sample Code**

	* 
