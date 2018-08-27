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
val context = PSContext.getOrCreate(spark.sparkContext)
val context = PSContext.instance()
// Stop PSContext
PSContext.stop()
```


### PSVector

PSVector is a subclass of PSModel, while PSVector has four different implementations of DensePSVector/SparsePSVector and BreezePSVector/CachedPSVector. DensePSVector or SparsePSVector is a PSVector of two different data formats, and BreezePSVector or CachedPSVector is a PSVector with two different functions.


Before introducing PSVector, you need to understand the concept of PSVectorPool first. PSVectorPool is not explicitly exposed in the programming interface of Spark on Angel, but understand its concept might be help for better programming.  

- PSVectorPool
  PSVectorPool is essentially a matrix on Angel PS, the number of matrix columns is `dim`, and the number of rows is `capacity`.
  PSVectorPool is responsible for PSVector application and automatic recycling. Automatically recycles GC functions similar to Java. PSVector objects do not need to be manually deleted after use.
  The dimensions of the PSVector in the same PSVectorPool are `dim`, and the PSVector in the same pool can be used for operations.

- PSVector application and initialization
  When PSVector is applied for the first time, it must be applied through the dose/sparse method in the associated object of PSVector.
  The dense/sparse method creates a PSVectorPool, so you need to pass in the dimension and capacity parameters.

  Through the duplicate method, you can apply for a PSVector with the same psVector object as the Pool.

```scala
    val dVector = PSVector.dense(dim, capacity)
    val sVector = PSVector.sparse(dim, capacity)

    val samePoolVector = PSVector.duplicate(dVector)

    dVector.fill(1.0)
    dVector.randomUniform(-1.0, 1.0)
    dVector.randomNormal(0.0, 1.0)
```

- **BreezePSVector**

- DensePSVector VS. SparsePSVector
  As the name suggests, DensePSVector and SparsePSVector are PSVectors designed for dense and sparse data formats.

- BreezePSVector VS. CachedPSVector
  BreezePSVector and CachedPSVector are PSVector decoration classes that encapsulate different computing functions.

  BreezePSVector is oriented to the Breeze algorithm library and encapsulates the operations between PSVectors in the same PSVectorPool. Including common math and blas operations, BreezePSVector implements the NumbericOps operation inside Breeze, so BreezePSVector supports operations such as +, -, *

   ```scala
    val brzVector1 = brzVector2 :* 2.0 + brzVector3
  ```
  You can also explicitly call the operations in Breeze.math and Breeze.blas.

  CachedPSVector provides Cache functionality for Pull, increment/mergeMax/mergeMin, reducing the number of interactions between these operations and PS.
  For example, pullWithCache will add the copied Vector cache to the local, and the next time the Pull is the same Vector, the cached Vector will be read directly;
  The incrementWithCache will aggregate the multiple increment operations locally, and finally the result of the local aggregation will be incremented to the PSVector through the flush operation.

  ```scala
  val cacheVector = PSVector.dense(dim).toCache
  rdd.map { case (label , feature) =>
      // 并没有立即更新psVector
    	cacheVector.increment(feature)
  }
  // flushIncrement会将所有executor上的缓存的cacheVector的increment结果，累加到cacheVector
  cacheVector.flushIncrement
  ```

## 5. PSMatrix
PSMatrix is ​​a matrix on Angel PS with two implementations, DensePSMatrix and SparsePSMatrix.

- PSMatrix creation and destruction
PSMatrix requests the corresponding matrix through the dense/sparse method in the companion object.
PSVector will have PSVectorPool to automatically recycle and destroy useless PSVector, while PSMatrix needs to manually call destroy method to destroy matrix on PS.

If you need to specify the partition parameters of PSMatrix, specify the size of each partition block by rowsInBlock/colsInBlock.

```scala
  // create, initialize
  Val dMatrix = DensePSMatrix.dense(rows, cols, rowsInBlock, colsInBlock)
  Val sMatrix = SparsePSMatrix.sparse(rows, cols)

  dMatrix.destroy()

  // Pull/Push operation
  Val vector = dMatrix.pull(rowId)
  dMatrix.push(rowId, vector)
```
## psFunc

- Spark on Angel supports psFunc just like Angel does, with even more powerful functional-programming features. psFunc inherits interfaces such as MapFunc and MapWithIndex to implement user-defined PSVector operations.

```scala
val result = brzVector.map(func)
val result = brzVector.mapWithIndex(func)
val result = brzVector.zipMap(func)
```
`func` above must inherit MapFunc and MapWithIndexFunc, and implement user-defined logic and serializable interface.


```scala
class MulScalar(scalar: Double, inplace: Boolean = false) extends MapFunc {
  def this() = this(false)

  setInplace(inplace)

  override def isOrigin: Boolean = true

  override def apply(elem: Double): Double = elem * scalar

  override def apply(elem: Float): Float = (elem * scalar).toFloat

  override def apply(elem: Long): Long = (elem * scalar).toLong

  override def apply(elem: Int): Int = (elem * scalar).toInt

  override def bufferLen(): Int = 9

  override def serialize(buf: ByteBuf): Unit = {
    buf.writeBoolean(inplace)
    buf.writeDouble(scalar)

  override def deserialize(buf: ByteBuf): Unit = {
    super.setInplace(buf.readBoolean())
    this.scalar = buf.readDouble()
  }
}
```

## examples


- Example 1： update for PSVector

aggregate features in RDD[(label, feature)] to PSVector:

```scala
val dim = 10
val capacity = 40

val psVector = PSVector.dense(dim, capacity).toCache

rdd.foreach { case (label , feature) =>
  psVector.increment(feature)
}
psVector.flushIncrement

println("feature sum:" + psVector.pull.asInstanceOf[IntDoubleVector].getStorage.getValues.mkString(" "))
```

- Example 2： implements for Gradient Descent

a simple version of Gradient Descent implemented by ps:

```scala

val w = PSVector.dense(dim).fill(initWeights)

for (i <- 1 to ITERATIONS) {
  val gradient = PSVector.duplicate(w)

  val nothing = instance.mapPartitions { iter =>
    val brzW = w.pull()

    val subG = iter.map { case (label, feature) =>
      feature.mul((1 / (1 + math.exp(-label * brzW.dot(feature))) - 1) * label)
    }.reduce(_ add _)

    gradient.increment(subG)
    Iterator.empty
  }
  nothing.count()

  w.toBreeze :+= gradent.toBreeze *:* -1.0
}

println("w:" + w.pull().asInstanceOf[IntDoubleVector].getStorage.getValues.mkString(" "))
```
