# Spark on Angel Programing Guide

Spark on Angel的算法实现与纯Spark的实现非常接近，因此大部分的Spark ML算法仅需要修改一小部分代码就能将算法跑到Spark on Angel上。

该版本的Spark on Angel是基于Spark 2.1.0和Scala 2.11.8，因此建议大家在该环境下开发。

开发者接触到的类主要有PSContext，PSModelPool，PSVectorProxy，BreezePSVector/RemotePSVector。
目前我们的编程接口以Scala为主，下面我们都将已Scala的编程方式介绍Spark on Angel的编程接口。

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
  import com.tencent.angel.spark.PSContext
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

// 此后，就无需传入SparkContext，直接的PSContext
val context = PSContext.getOrCreate()

// 终止PSContext
PSContext.stop()
```

- 申请/销毁PSModelPoool
PSModelPool在Angel PS上其实是一个矩阵，矩阵列数是dim，行数是capacity。
可以申请多个不同大小的PSModelPool。

```scala
val pool = context.createModelPool(dim, capacity)
context.destroyModelPool(pool)
```

### 4. PSModelPool
PSModelPool在Angel PS上其实是一个矩阵，矩阵列数是`dim`，行数是`capacity`。
同一个Application中，可以申请多个不同大小的PSModelPool。
可以从PSModelPool申请PSVector，存放在PSModle上PSVector的维度都是`dim`；
PSModelPool只能存放、管理维度为`dim`的PSVector。

注意：同一个Pool内的PSVector才能做运算。

```scala
// 用Array数据初始化一个PSVector，array的维度必须与pool维度保持一致
val arrayProxy = pool.createModel(array)
// PSVector的每个维度都是value
val valueProxy = pool.createModel(value)

// 全0的PSVector
val zeroProxy = pool.createZero()
// 随机的PSVector, 随机数服从均匀分布
val uniformProxy = pool.createRandomUniform(0.0, 1.0)
// 随机的PSVector, 随机数服从正态分布
val normalProxy = pool.createRandomNormal(0.0, 1.0)
```

使用之后的PSVector，可以手动delete、也可以放之不管系统会自动回收；delete后的PSVector就不能再使用。
```scala
pool.delete(vectorPorxy)
```

### 5. PSVectorProxy/PSVector
PSVectorProxy是PSVector（包括BreezePSVector和RemotePSVector）的代理，指向Angel PS上的某个PSVector。
而PSVector的BreezePSVector和RemotePSVector封装了在不同场景下的PSVector的运算。

- PSVectorProxy和PSVector（BreezePSVector和RemotePSVector）之间的转换

```scala
   // PSVectorProxy to BreezePSVector、RemotePSVector
  val brzVector = vectorProxy.mkBreeze()
  val remoteVector = vectorProxy.mkRemote()

  // BreezePSVector、RemotePSVector to PSVectorProxy
  val vectorProxy = brzVector.proxy
  val vectorProxy = remoteVector.proxy

  // BreezePSVector, RemotePSVector之间的转换
  val remoteVector = brzVector.toRemote()
  val brzVector = remoteVector.toBreeze()
```

- RemotePSVector
  RemotePSVector封装了PSVector和本地Array之间的操作

```scala
  // pull PSVector到本地
  val localArray = remoteVector.pull()
  // push 本地的Array到Angel PS
  remoteVector.push(localArray)
  // 将本地的Array累加到Angel PS上的PSVector
  remoteVector.increment(localArray)

  // 本地的Array和PSVector取最大值、最小值
  remoteVector.mergeMax(localArray)
  remoteVector.mergeMin(localArray)
```

- BreezePSVector
  BreezePSVector封装了同一个PSModelPool里PSVector之间的运算。包括常用的math运算和blas运算
  BreezePSVector实现了Breeze内部的NumbericOps操作，因此BreezePSVector支持+，-，* 这样的操作

```scala
  val brzVector1 = 2.0 * brzVector2 + brzVector3
```
也可以显式地调用Breeze.math和Breeze.blas里的操作。

### 6. 支持自定义的PS function

- 支持PSF（PS Function）自定义函数，继承MapFunc、MapWithIndexFunc等接口实现用户自定义的PSVector运算函数

```scala
val result = brzVector.map(func)
val result = brzVector.mapWithIndex(func)
val result = brzVector.zipMap(func)
```
以上的func必须继承MapFunc、MapWithIndexFunc，并实现用户自定义的逻辑和函数序列化接口。

```java
public class MulScalar implements MapFunc {
  private double multiplier;
  public MulScalar(double multiplier) {
    this.multiplier = multiplier;
  }

  public MulScalar() {
  }

  @Override
  public double call(double value) {
    return value * multiplier;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeDouble(multiplier);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    multiplier = buf.readDouble();
  }

  @Override
  public int bufferLen() {
    return 8;
  }

}
```

### 7 实战样例

- Example 1： PSVector的更新方式

下面是将RDD[(label, feature)]中的所有feature都累加到PSVector中。

```java
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

- Example 2： Gradient Descent实现

下面是一个简单版本的Gradient Descent的PS实现
```java
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
