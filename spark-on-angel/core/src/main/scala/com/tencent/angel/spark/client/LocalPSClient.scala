package com.tencent.angel.spark.client

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.tencent.angel.spark.{PSClient, PSVectorPool, PSVectorProxy}
import com.tencent.angel.spark.func._
import com.tencent.angel.spark.pool.BitSetPSVectorPool

/**
 * LocalPSClient is a local implement of PSClient. It plays a significant role in local mode debug.
 * LocalPSClient implements all "do*" operations.
 */
private[spark] class LocalPSClient extends PSClient {

  private val random = new Random(System.currentTimeMillis())

  private var poolIdCounter = 0

  private val storage = ArrayBuffer.empty[Array[Array[Double]]]

  private[spark] def getAngelConf(): Map[String, String] = Map.empty

  def fetchArray(vector: PSVectorProxy): Array[Double] = {
    storage(vector.poolId)(vector.id)
  }

  /**
   * Create a PSVectorPool, a PSClient can create more than one PSVectorPool
   */
  protected def doCreateVectorPool(numDimensions: Int, capacity: Int): PSVectorPool = {
    val arrays = Array.fill(capacity, numDimensions)(0.0)
    storage += arrays
    val pool = new BitSetPSVectorPool(this, poolIdCounter, numDimensions, capacity)
    poolIdCounter += 1
    pool
  }

  /**
   * Destroy a PSVectorPool
   */
  protected def doDestroyVectorPool(pool: PSVectorPool): Unit = {
    storage(pool.id) = null
    pool.destroy()
  }

  protected def doPut(to: PSVectorProxy, value: Array[Double]): Unit = {
    val target = fetchArray(to)
    val n = target.length
    Array.copy(value, 0, target, 0, n)
  }

  protected def doFill(to: PSVectorProxy, value: Double): Unit = {
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = value
    }
  }

  protected def doRandomUniform(to: PSVectorProxy, min: Double, max: Double): Unit = {
    val factor = max - min
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = factor * random.nextDouble() + min
    }
  }

  protected def doRandomNormal(to: PSVectorProxy, mean: Double, stddev: Double): Unit = {
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = stddev * random.nextGaussian() + mean
    }
  }

  protected def doSum(vector: PSVectorProxy): Double = {
    fetchArray(vector).sum
  }

  protected def doMax(vector: PSVectorProxy): Double = {
    fetchArray(vector).max
  }

  protected def doMin(vector: PSVectorProxy): Double = {
    fetchArray(vector).min
  }

  protected def doNnz(vector: PSVectorProxy): Int = {
    fetchArray(vector).count(_ != 0)
  }

  protected def doAdd(from: PSVectorProxy, value: Double, to: PSVectorProxy): Unit = {
    val source = fetchArray(from)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = source(i) + value
    }
  }

  protected def doMul(from: PSVectorProxy, value: Double, to: PSVectorProxy): Unit = {
    val source = fetchArray(from)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = source(i) * value
    }
  }

  protected def doDiv(from: PSVectorProxy, value: Double, to: PSVectorProxy): Unit = {
    val source = fetchArray(from)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = source(i) / value
    }
  }

  protected def doPow(from: PSVectorProxy, value: Double, to: PSVectorProxy): Unit = {
    val source = fetchArray(from)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = math.pow(source(i), value)
    }
  }

  protected def doSqrt(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    val source = fetchArray(from)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = math.sqrt(source(i))
    }
  }

  protected def doExp(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    val source = fetchArray(from)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = math.exp(source(i))
    }
  }

  protected def doExpm1(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    val source = fetchArray(from)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = math.expm1(source(i))
    }
  }

  protected def doLog(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    val source = fetchArray(from)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = math.log(source(i))
    }
  }

  protected def doLog1p(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    val source = fetchArray(from)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = math.log1p(source(i))
    }
  }

  protected def doLog10(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    val source = fetchArray(from)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = math.log10(source(i))
    }
  }

  protected def doCeil(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    val source = fetchArray(from)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = math.ceil(source(i))
    }
  }

  protected def doFloor(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    val source = fetchArray(from)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = math.floor(source(i))
    }
  }

  protected def doRound(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    val source = fetchArray(from)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = math.round(source(i))
    }
  }

  protected def doAbs(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    val source = fetchArray(from)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = math.abs(source(i))
    }
  }

  protected def doSignum(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    val source = fetchArray(from)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = math.signum(source(i))
    }
  }

  protected def doAdd(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      to: PSVectorProxy): Unit = {
    val source1 = fetchArray(from1)
    val source2 = fetchArray(from2)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = source1(i) + source2(i)
    }
  }

  protected def doSub(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      to: PSVectorProxy): Unit = {
    val source1 = fetchArray(from1)
    val source2 = fetchArray(from2)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = source1(i) - source2(i)
    }
  }

  protected def doMul(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      to: PSVectorProxy): Unit = {
    val source1 = fetchArray(from1)
    val source2 = fetchArray(from2)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = source1(i) * source2(i)
    }
  }

  protected def doDiv(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      to: PSVectorProxy): Unit = {
    val source1 = fetchArray(from1)
    val source2 = fetchArray(from2)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = source1(i) / source2(i)
    }
  }

  protected def doMax(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      to: PSVectorProxy): Unit = {
    val source1 = fetchArray(from1)
    val source2 = fetchArray(from2)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = math.max(source1(i), source2(i))
    }
  }

  protected def doMin(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      to: PSVectorProxy): Unit = {
    val source1 = fetchArray(from1)
    val source2 = fetchArray(from2)
    val target = fetchArray(to)
    for (i <- target.indices) {
      target(i) = math.min(source1(i), source2(i))
    }
  }

  protected def doMap(
      from: PSVectorProxy,
      func: MapFunc,
      to: PSVectorProxy): Unit = {
    val _from = fetchArray(from)
    val _to = fetchArray(to)
    for (i <- _from.indices) {
      _to(i) = func.call(_from(i))
    }
  }

  protected def doZip2Map(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      func: Zip2MapFunc,
      to: PSVectorProxy): Unit = {
    val _from1 = fetchArray(from1)
    val _from2 = fetchArray(from2)
    val _to = fetchArray(to)
    for (i <- _from1.indices) {
      _to(i) = func.call(_from1(i), _from2(i))
    }
  }

  protected def doZip3Map(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      from3: PSVectorProxy,
      func: Zip3MapFunc,
      to: PSVectorProxy): Unit = {
    val _from1 = fetchArray(from1)
    val _from2 = fetchArray(from2)
    val _from3 = fetchArray(from3)
    val _to = fetchArray(to)
    for (i <- _from1.indices) {
      _to(i) = func.call(_from1(i), _from2(i), _from3(i))
    }
  }

  protected def doMapWithIndex(
      from: PSVectorProxy,
      func: MapWithIndexFunc,
      to: PSVectorProxy): Unit = {
    val _from = fetchArray(from)
    val _to = fetchArray(to)
    for (i <- _from.indices) {
      _to(i) = func.call(i, _from(i))
    }
  }

  protected def doZip2MapWithIndex(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      func: Zip2MapWithIndexFunc,
      to: PSVectorProxy): Unit = {
    val _from1 = fetchArray(from1)
    val _from2 = fetchArray(from2)
    val _to = fetchArray(to)
    for (i <- _from1.indices) {
      _to(i) = func.call(i, _from1(i), _from2(i))
    }
  }

  protected def doZip3MapWithIndex(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      from3: PSVectorProxy,
      func: Zip3MapWithIndexFunc,
      to: PSVectorProxy): Unit = {
    val _from1 = fetchArray(from1)
    val _from2 = fetchArray(from2)
    val _from3 = fetchArray(from3)
    val _to = fetchArray(to)
    for (i <- _from1.indices) {
      _to(i) = func.call(i, _from1(i), _from2(i), _from3(i))
    }
  }

  protected def doAxpy(a: Double, x: PSVectorProxy, y: PSVectorProxy): Unit = {
    val _x = fetchArray(x)
    val _y = fetchArray(y)
    val n = x.numDimensions
    BLAS.daxpy(n, a, _x, 1, _y, 1)
  }

  protected def doDot(x: PSVectorProxy, y: PSVectorProxy): Double = {
    val _x = fetchArray(x)
    val _y = fetchArray(y)
    val n = x.numDimensions
    BLAS.ddot(n, _x, 1, _y, 1)
  }

  protected def doCopy(x: PSVectorProxy, y: PSVectorProxy): Unit = {
    val _x = fetchArray(x)
    val _y = fetchArray(y)
    val n = x.numDimensions
    Array.copy(_x, 0, _y, 0, n)
  }

  protected def doScal(a: Double, x: PSVectorProxy): Unit = {
    val _x = fetchArray(x)
    val n = x.numDimensions
    BLAS.dscal(n, a, _x, 1)
  }

  protected def doNrm2(x: PSVectorProxy): Double = {
    math.sqrt(fetchArray(x).map(x => x * x).sum)
  }

  protected def doAsum(x: PSVectorProxy): Double = {
    fetchArray(x).map(math.abs).sum
  }

  protected def doAmax(x: PSVectorProxy): Double = {
    fetchArray(x).map(math.abs).max
  }

  protected def doAmin(x: PSVectorProxy): Double = {
    fetchArray(x).map(math.abs).min
  }

  protected def doGet(vector: PSVectorProxy): Array[Double] = {
    fetchArray(vector).clone()
  }

  protected def doIncrement(vector: PSVectorProxy, delta: Array[Double]): Unit = {
    val target = fetchArray(vector)
    val n = target.length
    target.synchronized {
      BLAS.daxpy(n, 1.0, delta, 1, target, 1)
    }
  }

  protected def doMergeMax(vector: PSVectorProxy, other: Array[Double]): Unit = {
    val target = fetchArray(vector)
    target.synchronized {
      var i = 0
      while (i < target.length) {
        target(i) = math.max(target(i), other(i))
        i += 1
      }
    }
  }

  protected def doMergeMin(vector: PSVectorProxy, other: Array[Double]): Unit = {
    val target = fetchArray(vector)
    target.synchronized {
      var i = 0
      while (i < target.length) {
        target(i) = math.min(target(i), other(i))
        i += 1
      }
    }
  }
}
