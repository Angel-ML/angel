# Spark on Angel Programming Guide

We develop Spark-on-Angel to enable the programming of both Spark and Parameter Server. With Spark-on-Angel, you can not only employ the ability of Spark to couple with data cleaning, task scheculing, resource allocating and other distributed abilities from Spark, but also the Parameter Server ability of Angel to tackle high-dimensional models. Moreover, the programming on Spark-on-Angel is straightforward if you are familiar with Spark.

In its current version，Spark on Angel is developed with Spark 3.3.1 and Scala 2.12.15.


## Importing Spark on Angel

To write an application on Spark-on-Angel, in addition to the Spark dependency, you need to add the following Maven dependencies as well:

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
<dependency>
    <groupId>com.tencent.angel</groupId>
    <artifactId>spark-on-angel-graph</artifactId>
    <version>${angel.version}</version>
    <scope>provided</scope>
</dependency>
```

The corresponding import and implicit conversion:

```scala
  import com.tencent.angel.spark.context.PSContext
```

## Initializing Spark on Angel
To run an application of Spark-on-Angel, we should first start the Spark application and launch the servers of Angel. Hence, first we start Spark by the SparkSession (or SparkContext), and then start Angel servers with the ``PSContext`` class.

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

PSVector is a distributed vector stored on parameter servers. It is actually one row of the distributed matrix on servers.

PSVector is created from PSVectorPool. The PSVectorPool is actually a distributed matrix on servers while a PSVector is one row of this matrix. The proposal of PSVectorPool is to automatical manage the allocation and recycling of PSVector.
You can create a PSVector from a PSVectorPool and do not need to care when to remove it. The recycle mechanism of PSVector is similar to the Garbage Collection in Java.

You can create a dense PSVector by calling the interface of PSVector.
```scale
val dense = PSVector.dense(10, 5)
```
Thus, you create a dense PSVector with dimension 10. You also create a matrix (PSVectorPool) on servers with 5x10 shape. But the ``dense`` one only consume the firt row.

You can create a sparse PSVector by
```scala
val sparse = PSVector.sparse(10, 5)
```
The sparse PSVector employs hashmap for storage, which saving memory when your feature index is not continous.

Given a PSVector, we can conduct some operation on it. For example
```scala
// fill the dense vector with 1.0
dense.fill(1.0)
// randomly initializing the PSVector with normal distribution N(0.0, 1.0)
VectorUtils.randomNormal(dense, 0.0, 1.0)
// randomly initializing the PSVector with uniform distribution U(0.0, 1.0)
VectorUtils.randomUniform(dense, 0.0, 1.0)
```
One important interace of PSVector is duplicate. Through this interface, we
can allocate PSVectors from the same PSVectorPool. With two PSVectors from the same PSVectorPool (matrix), we can perform distributed operations between them, for example dot-product.

```scala
val denseFromSamePool = PSVector.duplicate(dense)
VectorUtils.randomNormal(denseFromSamePool, 0.0, 1.0)
val dot = VectorUtils.dot(dense, denseFromSamePool)
```

With a PSVector, we can pull/increment/update with it.
```scala
// pull the value from servers
val localVector1 = dense.pull()
// pull the value from servers with indices (integer type)
val localVector2 = dense.pull(Array(1, 2, 3))
// pull the value from servers with indices (long type)
val localVector3 = dense.pull(Array(1L, 2L, 3L))
// increment a PSVector
val delta = VFactory.sparseDoubleVector(10, Array(1, 2), Array(1.0, 1.0))
dense.increment(delta)
// update the vector
val update = VFactory.sparseDoubleVector(10, Array(1, 2), Array(0.0, 0.0))
dense.update(update)
```

## 5. PSMatrix
PSMatrix is a matrix stored on Angel servers. We develop the PSMatrix interface to facilitate the creatation and destroy of matrix.

You can create a dense PSMatrix by
```scala
val dense = PSMatrix.dense(rows, cols)
```
A sparse one can be created in a similar way
```scala
val sparse = PSMatrix.sparse(rows, cols)
```
To destroy a PSMatrix, call the destroy interface.
```scala
dense.destroy()
```
We can pull/increment/update a vector from PSMatrix
```scala
// pull the first row
dense.pull(0)
// pull the first with indices (0, 1)
dense.pull(0, Array(0, 1))

// update the first row with indices(0, 1)
val update = VFactory.sparseDoubleVector(10, Array(0, 1), Array(1.0, 1.0))
dense.update(0, update)

// reset the first row
dense.reset(0)
```

## psFunc

We develope a series of functions that can be executed on the servers. We call these functions ``psFunc``. Users can develope self-defined psFuncs by extending some interfaces. **shoule improved**

```scala
val to = PSVector.duplicate(vector)
val result = VectorUtils.map(vector, func, to)
val result = VectorUtils.mapWithIndex(vector, func, to)
val result = VectorUtils.zipMap(vector, func, to)
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

## Some Simple Examples

Example 1： Aggregate features in RDD[(label, feature)] to PSVector:

```scala
val dim = 10
val capacity = 1

// allocating a PSVector as the feature index
val features = PSVector.dense(dim, capacity)

rdd.foreach { case (label , feature) =>
  features.increment(feature)
}

println("feature sum:" + features.pull.asInstanceOf[IntDoubleVector].getStorage.getValues.mkString(" "))
```

- Example 2：A simple version of Gradient Descent:

```scala

// allocate the weight vector and initialize with 0
val weight = PSVector.dense(dim, 1).fill(0.0)
val gradient = PSVector.duplicate(weight)

val lr = 0.1

// parse input data, each example is a (label, Vector) pair
val instances = sc.textFile(path).map(f => parse(f))

for (i <- 1 to ITERATIONS) {
  gradient.fill(0.0)
  instance.foreachPartition { iter =>
    // pull the weight to local
    val brzW = w.pull()
    // calculate gradient
    val subG = VFactory.sparseDoubleVector(dim)
    iter.foreach { case (label, feature) =>
      subG.iadd(feature.mul((1 / (1 + math.exp(-label * brzW.dot(feature))) - 1) * label))
    }

    gradient.increment(subG)
  }
  // add gradient to weight
  VectorUtils.axpy(-lr, gradient, weight)
}
```

- Example 3: In this third example, we describe how to use the Adam optimizer to accelerate the convergence speed. To use the Adam optimizer, we need to maintain another two vectors to calculate the historical gradients. Compared with the vanilla SGD, which allocates a PSMatrix with two rows, we allocates a PSMatrix with 4 rows
```scala
import com.tencent.angel.ml.core.optimizer.Adam
import com.tencent.angel.ml.matrix.RowType


// allocate a randomly initialized 4xdim PS matrix
// the first row stores the weight values, the 2nd and 3rd rows stores the extra vectors for Adam
// while the 4th row stores the gradients gathered from workers
val mat = PSMatrix.rand(4, dim, RowType.T_FLOAT_DENSE)

// reset the 2,3 rows (rowId=1,2)
mat.reset(Array(1,2))

// use adam with lr = 0.01
val lr  = 0.01
val opt = Adam(lr)

// parse the input data, each example is a (label, Vector) pair
val instance = sc.textFile(path).map(f => parse(f))

for (iteration <- 1 until MAX_ITERATION) {
    // clear gradients at the start of each iteration
    mat.reset(3)

    val numSamples = data.sample(false, 0.01, 42).mapPartitions {
        case iter =>
            PSContext.instance()
            // pull the first row of matrix; we can use sparse pull here to save network communication, for example, mat.pull(0, Array(0,1,2))
            val weight = mat.pull(0)
            val gradients = VFactory.sparseFloatVector(dim)
            var size = 0
            iter.foreach { case (label, feature) =>
                gradients.iadd(feature.mul((1 / (1 + math.exp(-label * weight.dot(feature))) - 1) * label))
                size += 1
            }
            // add the gradient to servers, we add the gradients to the 4th row (rowId=3)
            mat.increment(3, gradient)
            Iterator.single(size)
    }.reduce(_ + _)
    // run the optimizer update on the driver,
    // the second param ``1`` means there are 1 row which is used for weights.
    opt.update(mat.id, 1, iteration, numSamples)
}

```