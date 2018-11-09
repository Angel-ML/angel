# Spark on Angel编程指南


Spark on Angel的算法实现与纯Spark的实现非常接近，因此大部分的Spark ML算法仅需要修改一小部分代码就能将算法跑到Spark on Angel上。

该版本的Spark on Angel是基于Spark 2.1.0和Scala 2.11.8，因此建议大家在该环境下开发。

开发者接触到的类主要有PSContext，PSVectorPool。 目前我们的编程接口以Scala为主，下面我们都将以Scala的编程方式介绍Spark on Angel的编程接口。

## 1. Spark on Angel的引入
- Maven工程的pom依赖

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
- import package

```scala
  import com.tencent.angel.spark.context.PSContext
```

## 2. 初始化Spark on Angel
首先必须启动Spark、初始化SparkSession，然后用SparkSession启动PSContext。
所有Spark、Angel PS相关的配置参数都set到builder，Angel PS会从SparkConf中得到用户的配置信息。

```scala
// 初始化Spark
val builder = SparkSession.builder()
  .master(master)
  .appName(appName)
  .config("spark.ps.num", "x")
  .config("B", "y")
val spark = builder.getOrCreate()

// 初始化Angel
val context = PSContext.getOrCreate(spark.sparkContext)
```

### 3. PSContext
系统将Angel PS的所有操作都封装到PSContext中，PSContext的操作主要包括以下几部分
- 初始化、终止PS node
如下的接口设计与Spark的SparkSession/sparkContext很接近。

```scala
// 第一次启动时，需要传入SparkContext
val context = PSContext.getOrCreate(spark.sparkContext)

// 此后，直接通过PSContext.instance()获取context
val context = PSContext.instance()

// 终止PSContext
PSContext.stop()
```

### 4. PSVector
PSVector是PSModel的子类

在介绍PSVector之前，需要先了解一下PSVectorPool的概念；PSVectorPool在Spark on Angel的编程接口中不会显式地接触到，但需要了解其概念。

- PSVectorPool  
  PSVectorPool本质上是Angel PS上的一个矩阵，矩阵列数是`dim`，行数是`capacity`。
  PSVectorPool负责PSVector的申请、自动回收。自动回收类似于Java的GC功能，PSVector对象使用后不用手动delete。
  同一个PSVectorPool里的PSVector的维度都是`dim`，同一个Pool里的PSVector才能做运算。

- PSVector的申请和初始化  
  PSVector第一次申请的时候，必须通过PSVector的伴生对象中dense/sparse方法申请。
  dense/sparse方法会创建PSVectorPool，因此需要传入dimension和capacity参数。

  通过duplicate方法可以申请一个与已有psVector对象同Pool的PSVector。

  ```scala
    // 第一次申请DensePSVector和SparsePSVector
    // capacity提供了默认参数
    val dVector = PSVector.dense(dim, capacity)
    val sVector = PSVector.sparse(dim, capacity)

    // 从现有的psVector duplicate出新的PSVector
    val samePoolVector = PSVector.duplicate(dVector)

    // 初始化
    // fill with 1.0
    dVector.fill(1.0)
    // 初始化dVector，使dVector的元素服从[-1.0, 1.0]的均匀分布
    VectorUtils.randomUniform(dVector, randomUniform(-1.0, 1.0), -1.0, 1.0)
    // 初始化dVector，使dVector的元素服从N(0.0, 1.0)的正态分布
    VectorUtils.randomNormal(dVector, 0.0, 1.0)
  ```

### 5. PSMatrix
PSMatrix是Angel PS上的矩阵。

- PSMatrix的创建和销毁   
PSMatrix通过伴生对象中的dense/sparse方法申请对应的matrix。
PSVector会有PSVectorPool自动回收、销毁无用的PSVector，而PSMatrix需要手动调用destroy方法销毁PS上的matrix

如果需要对指定PSMatrix的分区参数，通过rowsInBlock/colsInBlock指定每个分区block的大小。

```scala
  // 创建、初始化
  val dMatrix = DensePSMatrix.dense(rows, cols, rowsInBlock, colsInBlock)
  val sMatrix = SparsePSMatrix.sparse(rows, cols)

  dMatrix.destroy()

  // Pull/Push操作
  val vector = dMatrix.pull(rowId)
  dMatrix.push(rowId, vector)
```

### 6. 支持自定义的PS function

- 支持PSF（PS Function）自定义函数，继承MapFunc、MapWithIndexFunc等接口实现用户自定义的PSVector运算函数

```scala
val to = PSVector.duplicate(vector)
val result = VectorUtils.map(vector, func, to)
val result = VectorUtils.mapWithIndex(vector, func, to)
val result = VectorUtils.zipMap(vector, func, to)
```
以上的func必须继承MapFunc、MapWithIndexFunc，并实现用户自定义的逻辑和函数序列化接口。

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

### 7 实战样例

- Example 1： PSVector的更新方式

下面是将RDD[(label, feature)]中的所有feature都累加到PSVector中。

```scala
val dim = 10
val capacity = 40

val psVector = PSVector.dense(dim, capacity)

rdd.foreach { case (label , feature) =>
  psVector.increment(feature)
}

println("feature sum:" + psVector.pull.asInstanceOf[IntDoubleVector].getStorage.getValues.mkString(" "))
```

- Example 2： Gradient Descent实现

下面是一个简单版本的Gradient Descent的PS实现，
注：这个例子里的instance的label是-1和1。

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
  
  VectorUtils.axpy(-1.0, gradient, w)
}

println("w:" + w.pull().asInstanceOf[IntDoubleVector].getStorage.getValues.mkString(" "))
```
