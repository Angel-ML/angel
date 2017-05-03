package com.tencent.angel.spark.client

import com.tencent.angel.spark._
import com.tencent.angel.spark.func._
import org.apache.spark.rdd.RDD

class LocalPSClientOpsSuite extends PSFunSuite with SharedPSContext {

  // PS constant parameter
  val dim = 10
  val capacity = 10
  val rddSize = 10
  var _localClient: LocalPSClient = _
  var _pool: PSVectorPool = _
  var _rdd: RDD[Array[Double]] = _

  var psProxy: PSVectorProxy = _
  var zeroProxy: PSVectorProxy = _
  var uniformProxy: PSVectorProxy = _
  var normalProxy: PSVectorProxy = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    _localClient = PSClient.get.asInstanceOf[LocalPSClient]
    _pool = _localClient.createVectorPool(dim, capacity)
    val thisDim = dim
    _rdd = sc.parallelize(0 until rddSize, 2)
      .map (i => Array.fill(thisDim)(i.toDouble))
    _rdd.cache()
    _rdd.count()
  }

  override def afterAll(): Unit = {
    _rdd.unpersist()
    _pool.destroy()
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    psProxy = _pool.createZero()
    zeroProxy = _pool.createZero()
    uniformProxy = _pool.createRandomUniform(0.0, 1.0)
    normalProxy = _pool.createRandomNormal(0.0, 1.0)
  }

  override def afterEach(): Unit = {
    _pool.delete(normalProxy)
    _pool.delete(uniformProxy)
    _pool.delete(zeroProxy)
    _pool.delete(psProxy)
    super.afterEach()
  }

  test("doGet") {
    val localArray = normalProxy.mkLocal().get()
    assert(_localClient.get(normalProxy).sameElements(localArray))
  }

  test("doIncrement") {
    val psProxy = _pool.createZero()
    _rdd.foreach { array =>
      val client = PSClient.get.asInstanceOf[LocalPSClient]
      client.increment(psProxy, array)
    }

    val sum = (0 until rddSize).sum
    assert(psProxy.mkLocal().get().sameElements(Array.fill[Double](dim)(sum)))
  }

  test("doMergeMax") {
    val psProxy = _pool.createZero()
    _rdd.foreach { array =>
      val client = PSClient.get.asInstanceOf[LocalPSClient]
      client.mergeMax(psProxy, array)
    }
    val max = rddSize - 1
    assert(psProxy.mkLocal().get().sameElements(Array.fill[Double](dim)(max)))
  }

  test("doMergeMin") {
    val psProxy = _pool.createZero()
    _rdd.foreach { array =>
      val client = PSClient.get.asInstanceOf[LocalPSClient]
      client.mergeMin(psProxy, array)
    }
    val min = 0.0
    assert(psProxy.mkLocal().get().sameElements(Array.fill[Double](dim)(min)))
  }

  test("doPut") {
    val array = Array.fill[Double](dim)(3.14)
    val psProxy = _pool.createZero()

    _localClient.put(psProxy, array)
    assert(psProxy.mkLocal().get().sameElements(Array.fill[Double](dim)(3.14)))
  }

  test("doFill") {
    val psProxy = _pool.createZero()
    _localClient.fill(psProxy, 3.1415)

    assert(psProxy.mkLocal().get().sameElements(Array.fill(dim)(3.1415)))
  }

  test("doRandomUniform") {
    val psProxy = _pool.createZero()
    _localClient.randomUniform(psProxy, 0.0, 1.0)

    psProxy.mkLocal().get().foreach { x =>
      assert(x < 1.0 && x > 0.0)
    }
  }

  test("doRandomNormal") {
    val pool = _localClient.createVectorPool(10000, 2)
    val psProxy = pool.createZero()
    _localClient.randomNormal(psProxy, 0.0, 1.0)
    val array = psProxy.mkLocal().get()

    val mean = array.sum / array.length
    val variety = array.map(x => math.pow(x - mean, 2.0)).sum / (array.length - 1)

    val tol = 0.1
    assert(math.abs(mean - 0.0) < tol)
    assert(math.abs(math.sqrt(variety) - 1.0) < tol)

    _localClient.destroyVectorPool(pool)
  }

  test("doSum") {
    assert(uniformProxy.mkLocal().get().sum === _localClient.sum(uniformProxy))
  }

  test("doMax") {
    assert(normalProxy.mkLocal().get().max === _localClient.max(normalProxy))
  }

  test("doMin") {
    assert(normalProxy.mkLocal().get().min === _localClient.min(normalProxy))
  }

  test("doNnz") {
    val array = Array.fill[Double](dim)(0.0)
    array.update(3, 1.0)
    array.update(0, 1.0)
    _localClient.put(psProxy, array)

    assert(_localClient.nnz(psProxy) == 2)
  }

  test("add const number") {
    val constNum = 3.14
    _localClient.add(uniformProxy, constNum, psProxy)

    val result = uniformProxy.mkLocal().get().map(_ + constNum)
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("multiply const number") {
    val constNum = 3.14
    _localClient.mul(uniformProxy, constNum, psProxy)

    val result = uniformProxy.mkLocal().get().map(_ * constNum)
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("divide const number") {
    val constNum = 3.14
    _localClient.div(uniformProxy, constNum, psProxy)

    val result = uniformProxy.mkLocal().get().map(_ / constNum)
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("doPow") {
    val constNum = 3.14
    _localClient.pow(uniformProxy, constNum, psProxy)

    val result = uniformProxy.mkLocal().get().map(math.pow(_, constNum))
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("doSqrt") {
    _localClient.sqrt(uniformProxy, psProxy)

    val result = uniformProxy.mkLocal().get().map(x => math.sqrt(x))
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("doExp") {
    _localClient.exp(uniformProxy, psProxy)

    val result = uniformProxy.mkLocal().get().map(x => math.exp(x))
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("doExpm1") {
    _localClient.expm1(uniformProxy, psProxy)

    val result = uniformProxy.mkLocal().get().map(x => math.expm1(x))
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("doLog") {
    _localClient.log(uniformProxy, psProxy)

    val result = uniformProxy.mkLocal().get().map(x => math.log(x))
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("doLog1p") {
    _localClient.log1p(uniformProxy, psProxy)

    val result = uniformProxy.mkLocal().get().map(x => math.log1p(x))
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("doLog10") {
    _localClient.log10(uniformProxy, psProxy)

    val result = uniformProxy.mkLocal().get().map(x => math.log10(x))
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("doCeil") {
    _localClient.ceil(uniformProxy, psProxy)

    val result = uniformProxy.mkLocal().get().map(x => math.ceil(x))
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("doFloor") {
    _localClient.floor(uniformProxy, psProxy)

    val result = uniformProxy.mkLocal().get().map(x => math.floor(x))
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("doRound") {
    _localClient.round(uniformProxy, psProxy)

    val result = uniformProxy.mkLocal().get().map(x => math.round(x))
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("doAbs") {
    _localClient.abs(uniformProxy, psProxy)

    val result = uniformProxy.mkLocal().get().map(x => math.abs(x))
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("doSignum") {
    _localClient.signum(uniformProxy, psProxy)

    val result = uniformProxy.mkLocal().get().map(x => math.signum(x))
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("add two vector") {
    _localClient.add(uniformProxy, normalProxy, psProxy)

    val uniArray = uniformProxy.mkLocal().get()
    val normalArray = normalProxy.mkLocal().get()
    val result = (0 until dim).toArray.map(i => uniArray(i) + normalArray(i))
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("sub two vector") {
    _localClient.sub(uniformProxy, normalProxy, psProxy)

    val uniArray = uniformProxy.mkLocal().get()
    val normalArray = normalProxy.mkLocal().get()
    val result = (0 until dim).toArray.map(i => uniArray(i) - normalArray(i))
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("multiply two vector") {
    _localClient.mul(uniformProxy, normalProxy, psProxy)

    val uniArray = uniformProxy.mkLocal().get()
    val normalArray = normalProxy.mkLocal().get()
    val result = (0 until dim).toArray.map(i => uniArray(i) * normalArray(i))
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("divide two vector") {
    _localClient.div(normalProxy, uniformProxy, psProxy)

    val normalArray = normalProxy.mkLocal().get()
    val uniArray = uniformProxy.mkLocal().get()
    val result = (0 until dim).toArray.map(i => normalArray(i) / uniArray(i))
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("max element of two vector") {
    _localClient.max(uniformProxy, normalProxy, psProxy)

    val uniArray = uniformProxy.mkLocal().get()
    val normalArray = normalProxy.mkLocal().get()
    val result = (0 until dim).toArray.map (i =>
      if (uniArray(i) > normalArray(i)) uniArray(i) else normalArray(i)
    )
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("min element of two vector") {
    _localClient.min(uniformProxy, normalProxy, psProxy)

    val uniArray = uniformProxy.mkLocal().get()
    val normalArray = normalProxy.mkLocal().get()
    val result = (0 until dim).toArray.map (i =>
      if (uniArray(i) < normalArray(i)) uniArray(i) else normalArray(i)
    )
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("map function") {
    val multiplier = 2.0
    _localClient.map(normalProxy, new MapFuncTest(multiplier), psProxy)

    val result = normalProxy.mkLocal().get().map(x => multiplier * x * x)
    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("zip2Map function") {
    val multiplier = 2.0
    _localClient.zip2Map(uniformProxy, normalProxy, new Zip2MapFuncTest(multiplier), psProxy)

    val uniformArray = uniformProxy.mkLocal().get()
    val normalArray = normalProxy.mkLocal().get()
    val result = (0 until dim).toArray.map { i =>
      multiplier * uniformArray(i) + normalArray(i) * normalArray(i)
    }

    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("zip3Map function") {
    val multiplier = 2.0
    _localClient.zip3Map(uniformProxy, normalProxy, normalProxy, new Zip3MapFuncTest(multiplier), psProxy)

    val uniformArray = uniformProxy.mkLocal().get()
    val normalArray = normalProxy.mkLocal().get()
    val result = (0 until dim).toArray.map { i =>
      multiplier * uniformArray(i) * (1 - normalArray(i)) + normalArray(i) * normalArray(i)
    }

    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("mapWithIndex function") {
    val multiplier = 2.0
    _localClient.mapWithIndex(normalProxy, new MapWithIndexFuncTest(multiplier), psProxy)

    val normalArray = normalProxy.mkLocal().get()
    val result = (0 until dim).toArray.map { i =>
      if (i == 0) {
        normalArray(i) * normalArray(i)
      } else {
        multiplier * normalArray(i) * normalArray(i)
      }
    }

    assert(psProxy.mkLocal().get().sameElements(result))
  }


  test("zip2MapWithIndex function") {
    val multiplier = 2.0
    _localClient.zip2MapWithIndex(uniformProxy, normalProxy, new Zip2MapWithIndexFuncTest(multiplier), psProxy)

    val uniformArray = uniformProxy.mkLocal().get()
    val normalArray = normalProxy.mkLocal().get()
    val result = (0 until dim).toArray.map {i =>
      if (i == 0) {
        uniformArray(i) + normalArray(i) * normalArray(i)
      } else {
        multiplier * uniformArray(i) + normalArray(i) * normalArray(i)
      }
    }

    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("zip3MapWithIndex function") {
    val multiplier = 2.0
    _localClient.zip3MapWithIndex(uniformProxy, normalProxy, normalProxy, new Zip3MapWithIndexFuncTest(multiplier), psProxy)

    val uniformArray = uniformProxy.mkLocal().get()
    val normalArray = normalProxy.mkLocal().get()
    val result = (0 until dim).toArray.map { i =>
      if (i == 0) {
        uniformArray(i) * (1 - normalArray(i)) + normalArray(i) * normalArray(i)
      } else {
        multiplier * uniformArray(i) * (1 - normalArray(i)) + normalArray(i) * normalArray(i)

      }
    }

    assert(psProxy.mkLocal().get().sameElements(result))
  }

  test("doAxpy") {
    val uniformArray = uniformProxy.mkLocal().get()
    val normalArray = normalProxy.mkLocal().get()
    val result = (0 until dim).toArray.map { i =>
      normalArray(i) + 2.0 * uniformArray(i)
    }

    _localClient.axpy(2.0, uniformProxy, normalProxy)

    assert(normalProxy.mkLocal().get().sameElements(result))
  }

  test("doDot") {
    val dot = _localClient.dot(uniformProxy, normalProxy)
    val uniformArray = uniformProxy.mkLocal().get()
    val normalArray = normalProxy.mkLocal().get()
    val result = (0 until dim).toArray.map { i =>
      normalArray(i) * uniformArray(i)
    }.sum
    assert(dot === result)
  }

  test("doCopy") {
    _localClient.copy(normalProxy, psProxy)

    assert(psProxy.mkLocal().get().sameElements(normalProxy.mkLocal().get()))
  }

  test("doScal") {
    val result = normalProxy.mkLocal().get().map(_ * -0.1)
    _localClient.scal(-0.1, normalProxy)

    assert(normalProxy.mkLocal().get().sameElements(result))
  }

  test("doNrm2") {
    val norm = _localClient.nrm2(normalProxy)
    val result = math.sqrt(normalProxy.mkLocal().get().map(x => x * x).sum)
    assert(result === norm)
  }

  test("doAsum") {
    val asum = _localClient.asum(normalProxy)
    val result = normalProxy.mkLocal().get().map(math.abs).sum
    assert(result === asum)
  }

  test("doAmax") {
    val amax = _localClient.amax(normalProxy)
    val result = normalProxy.mkLocal().get().map(math.abs).max
    assert(result === amax)
  }

  test("doAmin") {
    val amin = _localClient.amin(normalProxy)
    val result = normalProxy.mkLocal().get().map(math.abs).min
    assert(result === amin)
  }
}
