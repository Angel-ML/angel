# Spark on Angel Programming Guide

We develop Spark-on-Angel to enable the programming of both Spark and Parameter Server. With Spark-on-Angel, you can not only employ the ability of Spark to couple with data cleaning, task scheculing, resource allocating and other distributed abilities from Spark, but also the Parameter Server ability of Angel to tackle high-dimensional models. Moreover, the programming on Spark-on-Angel is straightforward if you are familiar with Spark.

In its current version，Spark on Angel is developed with Spark 2.1.1 and Scala 2.11.8.


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

Example 3: In this third example, we describe how to use the Adam optimizer to accelerate the convergence speed. To use the Adam optimizer, we need to maintain another two vectors to calculate the historical gradients. Compared with the vanilla SGD, which allocates a PSMatrix with two rows, we allocates a PSMatrix with 4 rows
```scala
import com.tencent.angel.ml.core.optimizer.Adam
import com.tencent.angel.ml.math2.utils.RowType


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


## Model Train
Here we descripe how to train a model, such as Logistic Regression, Factorization Machine, in Spark on Angel. Using the interfaces of PSVector is kinds of complicated.
Hence, we can use the interfaces of AngelGraph in Spark-on-Angel to simplify the programming for machine learning algorithms.


### Writing a Logisitc Regression Graph
To write a Logistic Regression algorithm, we write a class ``LogisticRegression`` and extends the GraphModel interface in Spark-on-Angel. In this class, we write two layers, including a ``SimpleInputLayer`` and a ``SimpleLossLayer``.

```scala
package com.tencent.angel.spark.ml.classification

import com.tencent.angel.ml.core.conf.AngelMLConf
import com.tencent.angel.ml.core.network.layers.verge.{SimpleLossLayer, SimpleInputLayer}
import com.tencent.angel.ml.core.network.transfunc.Identity
import com.tencent.angel.ml.core.optimizer.Adam
import com.tencent.angel.ml.core.optimizer.loss.LogLoss
import com.tencent.angel.spark.ml.core.GraphModel

class LogisticRegression extends GraphModel {

  // get the learning rate
  val lr = conf.getDouble(MLConf.ML_LEARN_RATE)

  override
  def network(): Unit = {
    // data input layer.
    // @input is the name of this layer
    // @1 is the output dimmension for this layer. We set is as 1 since Logistic Regression is a binary classification task.
    // @Identify() is the transition function, Identity means we do nothing for the output of the input layer.
    // @Adam(lr) is the optimizer, we here use Adam algorith with learning rate lr.
    val input = new SimpleInputLayer("input", 1, new Identity(), new Adam(lr))

    // loss layer
    // @simpleLossLayer is the name of this layer
    // @input means that we use the ``input`` layer as the input of this layer
    // @LogLoss is the loss function for this layer
    new SimpleLossLayer("simpleLossLayer", input, new LogLoss)
  }
}
```

### Parsing the input data
To pass the data into the SimpleInputLayer, we need to convert the data as ``LabeledData``, which is the data type in Angel. A simple example of parsing the data is
given as the following example. Assuming that the input format is ``libsvm``.

```scala
def parseIntFloat(text: String, dimension: Int): LabeledData = {
    if (null == text)
      return null

    var splits = text.trim.split(" ")

    if (splits.length < 1)
      return null

    var y = splits(0).toDouble
    // For the classification algorithm, we should convert the label as (-1,1)
    if (y == 0.0) y = -1.0

    splits = splits.tail
    val len = splits.length

    val keys: Array[Int] = new Array[Int](len)
    val vals: Array[Float] = new Array[Float](len)

    splits.zipWithIndex.foreach { case (value: String, indx2: Int) =>
      val kv = value.trim.split(":")
      keys(indx2) = kv(0).toInt
      vals(indx2) = kv(1).toFloat
    }
    // Using the VFactory interface to create a IntFloatVector
    val x = VFactory.sparseFloatVector(dim, keys, vals)
    new LabeledData(x, y)
  }
```

### Train the model
To train a Logistic Regression model, we write a ``train`` function with the parsed data and LogisticRegression model as input.

```scala
def train(data: RDD[LabeledData], model: LogisticRegression): Unit = {
    // broadcast the model to all executors
    val bModel = SparkContext.getOrCreate().broadcast(model)

    for (iteration <- 0 until numIteration) {
        val (sumLoss, batchSize) = data.sample(fraction, false, 42).mapPartition { case iter =>
            // Call PSContext to initialize the connection with servers.
            PSContext.instance()
            val batch = iter.toArray()
            bModel.value.forward(epoch, batch)
            val loss = bModel.value.getLoss()
            bModel.value.backward()
            Iterator.single((loss, batch.length))
        }.reduce((f1, f2) => (f1._1 + f2._1, f1._2 + f2._2))

        // update the model with Adam optimizer
        model.update(iteration,  batchSize)
        val loss = sumLoss / model.graph.taskNum
        println(s"epoch=[$epoch] lr[$lr] batchSize[$batchSize] trainLoss=$loss")
    }
}
```

### Putting all things together
```scala
// load data
val conf = new SparkConf()
val sc   = new SparkContext(conf)
val data = sc.textFile(input).map(f => parseIntFloat(f, dim))

// set running mode, use angel_ps mode for spark
SharedConf.get().set(AngelConf.ANGEL_RUNNING_MODE, RunningMode.ANGEL_PS.toString)

// start PS
PSContext.getOrCreate(sc)

// model
val model = new LogisticRegression()
// initialize the model
model.init(data.getNumPartitions)
// training
train(data, model)

// save model
val path = 'hdfs://xxx'
model.save(path)
```

### Saving model and loading model
Spark on Angel can save/load the model from HDFS. Hence, we can complete increment training and model predict.
```scala
// saving a model
model.save(path)
// loading a model
model.load(path)
```

### Predict with data
To predict data, we only need to do the forward pass and output the sigmoid value.
```scala
// first load the model
val model = new LogisticRegression()
model.load(path)

// broadcast model
val bModel = SparkContext.getOrCreate().broadcast(model)
// predict
data.mapPartitions { case iter =>
    val samples = iterator.toArray
    val output  = bModel.value.forward(1, samples)
    // the output is matrix. For each example, it calculate (output, sigmoid, label)
    (output) match {
        case (mat: BlasDoubleMatrix) =>
          (0 until mat.getNumRows).map(idx => (mat.get(idx, 1))).iterator
        case (mat: BlasFloatMatrix) =>
          (0 until mat.getNumRows).map(idx => (mat.get(idx, 1).toDouble)).iterator
    }
}
```


### Factorization Machine
For FM algorithm, we only need to write an another class that extends the GraphModel interface while the other steps are all the same.
```scala
package com.tencent.angel.spark.ml.classification

import com.tencent.angel.ml.core.conf.AngelMLConf
import com.tencent.angel.ml.core.network.layers.verge.{Embedding, SimpleLossLayer, SimpleInputLayer}
import com.tencent.angel.ml.core.network.layers.join.SumPooling
import com.tencent.angel.ml.core.network.layers.linear.BiInnerSumCross
import com.tencent.angel.ml.core.network.transfunc.Identity
import com.tencent.angel.ml.core.optimizer.{Adam, Momentum}
import com.tencent.angel.ml.core.optimizer.loss.LogLoss
import com.tencent.angel.spark.ml.core.GraphModel

class FactorizationMachine extends GraphModel {

  val numField: Int = conf.getInt(MLConf.ML_FIELD_NUM)
  val numFactor: Int = conf.getInt(MLConf.ML_RANK_NUM)
  val lr: Double = conf.getDouble(MLConf.ML_LEARN_RATE)

  override
  def network(): Unit = {
    val wide = new SimpleInputLayer("wide", 1, new Identity(), new Adam(lr))
    val embedding = new Embedding("embedding", numField * numFactor, numFactor, new Adam(lr))
    val crossFeature = new BiInnerSumCross("innerSumPooling", embedding)
    val sum = new SumPooling("sum", 1, Array(wide, crossFeature))
    new SimpleLossLayer("simpleLossLayer", sum, new LogLoss)
  }
}
```