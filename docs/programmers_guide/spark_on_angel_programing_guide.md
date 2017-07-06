# Spark on Angel编程指南（Spark on Angel Programing Guide）

Spark on Angel设计的目的和初衷，就是让大部分的Spark开发者，只需要很小的代价和修改，就能切换到Spark on Angel上。所以Spark on Angel的算法实现与纯Spark的实现非常接近，大部分的Spark ML算法仅需要修改一小部分代码就能将算法跑到Spark on Angel上

目前，Spark on Angel是基于Spark 2.1.1和Scala 2.11.8


## 引入Spark on Angel

编写Spark on Angel，除了Spark的依赖之外，你需要额外加入如下Maven依赖：

```xml
<dependency>
    <groupId>com.tencent.angel</groupId>
    <artifactId>spark-on-angel-core</artifactId>
    <version>${angel.version}</version>
</dependency>
<dependency>
    <groupId>com.tencent.angel</groupId>
    <artifactId>spark-on-angel-mllib</artifactId>
    <version>${angel.version}</version>
</dependency>
```

相应的Import和隐式转换

```scala
  import com.tencent.angel.spark._
  import com.tencent.angel.spark.PSContext
```

## 初始化Spark on Angel

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

// 初始化Angel的PSContext
val context = PSContext.getOrCreate(spark.sparkContext)
```

Angel PS中，Driver端的所有操作都封装到PSContext中，初始化和终止PS Server的接口和Spark的SparkSession/sparkContext很接近。

```scala
// 终止PSContext
PSContext.stop()
```

## PSModelPool

对应于Angel的PSModel，Spark on Angel的核心抽象是`PSModelPool`，和PSModel不同，为了Spark进行互动，PSModelPool进行了微妙的改进和调整。值得留意的是：

> 对于Angel来说，在PSServer上，无论是Angel还是Spark on Angel的客户端，都是一视同仁的



PSModelPool在Angel PS上其实是一个矩阵，矩阵列数是`dim`，行数是`capacity`。同一个Application中，可以申请多个不同大小的PSModelPool。它的概念，其实对标于Angel里面的`PSModel`。

* 创建ModelPool

```scala
val pool = context.createModelPool(dim, capacity)
```

* 销毁ModelPool

```scala
context.destroyModelPool(pool)
```


### PSVectorProxy

一个PSModelPool，可以申请PSVector，存放在PSModel上PSVector的维度都是`dim`；PSModelPool只能存放、管理维度为`dim`的PSVector。

PSVectorProxy是PSVector（包括BreezePSVector和RemotePSVector）的代理，指向Angel PS上的某个PSVector。


```scala
// 创建一个PSVector，array的维度必须与pool维度保持一致
val arrayProxy = pool.createModel(array)
// 创建一个PSVector，Vector的每个维度值都是value
val valueProxy = pool.createModel(value)
// 创建一个全0的PSVector
val zeroProxy = pool.createZero()
// 创建一个随机的PSVector, 随机数服从均匀分布
val uniformProxy = pool.createRandomUniform(0.0, 1.0)
// 创建一个随机的PSVector, 随机数服从正态分布
val normalProxy = pool.createRandomNormal(0.0, 1.0)
```

使用之后的PSVectorProxy，可以手动delete，也可以等待系统会自动回收。手工delete后的PSVectorProxy就不能再使用。

```scala
pool.delete(vectorProxy)
```

### PSVector

BreezePSVector和RemotePSVector都是PSVector的子类，封装了在不同场景下的PSVector的运算。

- **RemotePSVector**
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

- **BreezePSVector**

BreezePSVector封装了同一个PSModelPool里PSVector之间的运算。包括常用的math运算和blas运算。BreezePSVector实现了Breeze内部的NumbericOps操作，因此BreezePSVector支持+，-，* 这样的操作

```scala
  val brzVector1 = (brzVector2 :* 2) + brzVector3
```

也可以显式地调用Breeze.math和Breeze.blas里的操作。

- **互相转换**


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

## psFunc（自定义函数）

- 在Spark on Angel中，和Angel一样，psFunc也是被支持的，而且力度更大，在函数式上走得更远。继承MapFunc、MapWithIndexFunc等接口实现用户自定义的PSVector运算函数

```scala
val result = brzVector.map(func)
val result = brzVector.mapWithIndex(func)
val result = brzVector.zipMap(func)
```
以上的func必须继承MapFunc、MapWithIndexFunc，并实现用户自定义的逻辑和函数序列化接口。


## 样例代码

1. **PSVector的更新**

	将RDD[(label, feature)]中的所有feature都累加到PSVector中。


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

2. **Gradient Descent实现**

	最简版本Gradient Descent的Spark on Angel实现

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
	
3. **更多的样例代码**

	* 
