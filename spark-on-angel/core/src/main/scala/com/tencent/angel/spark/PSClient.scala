package com.tencent.angel.spark

import java.util.concurrent.ConcurrentHashMap

import scala.collection.Map

import com.github.fommil.netlib.F2jBLAS
import org.apache.spark._

import com.tencent.angel.AngelDeployMode
import com.tencent.angel.spark.client.{AngelPSClient, LocalPSClient}
import com.tencent.angel.spark.func._

/**
 * PSClient is a client which contains operations for PSVector on the PS nodes.
 * These operations can be called on the Spark driver or executor.
 */
abstract class PSClient {
  import PSClient._

  private[spark] val BLAS = new F2jBLAS

  /**
   * PSClient can create more than one PSVectorPool.
   */
  private val psVectorPools = new ConcurrentHashMap[Int, PSVectorPool]()

  def getPool(id: Int): PSVectorPool = {
    psVectorPools.get(id)
  }

  /* =========================================== */
  /*          driver only methods                */
  /* =========================================== */

  /**
   * Create a vector pool in PS nodes.
   * Notice: it can only be called on driver.
   *
   * @param numDimensions dimension of vectors
   * @param capacity capacity of pool
   */
  def createVectorPool(
      numDimensions: Int,
      capacity: Int): PSVectorPool = {
    assertOnDriver()
    val pool = doCreateVectorPool(numDimensions, capacity)
    psVectorPools.put(pool.id, pool)
    pool
  }

  /**
   * Destroy a vector pool in PS nodes.
   * Notice: it can only be called in th driver.
   *
   * @param pool the pool to destroy
   */
  def destroyVectorPool(pool: PSVectorPool): Unit = {
    assertOnDriver()
    psVectorPools.remove(pool.id)
    doDestroyVectorPool(pool)
  }

  /**
   * Put `value` to PSVectorKey
   * Notice: it can only be called in th driver.
   */
  def put(to: PSVectorProxy, value: Array[Double]): Unit = {
    assertOnDriver()
    to.assertValid()
    to.assertCompatible(value)
    doPut(to, value)
  }

  /**
   * Fill PSVectorKey with `value`
   * Notice: it can only be called in th driver.
   */
  def fill(to: PSVectorProxy, value: Double): Unit = {
    assertOnDriver()
    to.assertValid()
    doFill(to, value)
  }

  /**
   * Generate a random PSVector, the random distribution is uniform.
   * Notice: it can only be called in th driver.
   *
   * @param min the minimum of uniform distribution
   * @param max the maximum of uniform distribution
   */
  def randomUniform(to: PSVectorProxy, min: Double, max: Double): Unit = {
    assertOnDriver()
    to.assertValid()
    doRandomUniform(to, min, max)
  }

  /**
   * Generate a random PSVector, the random distribution is normal distribution.
   * Notice: it can only be called in th driver.
   *
   * @param mean the `mean` parameter of uniform distribution
   * @param stddev the `stddev` parameter of uniform distribution
   */
  def randomNormal(to: PSVectorProxy, mean: Double, stddev: Double): Unit = {
    assertOnDriver()
    to.assertValid()
    doRandomNormal(to, mean, stddev)
  }

  /**
   * Sum all the dimension of `vector`
   * Notice: it can only be called in th driver.
   */
  def sum(vector: PSVectorProxy): Double = {
    assertOnDriver()
    vector.assertValid()
    doSum(vector)
  }

  /**
   * Find the maximum element of `vector`
   * Notice: it can only be called in th driver.
   */
  def max(vector: PSVectorProxy): Double = {
    assertOnDriver()
    vector.assertValid()
    doMax(vector)
  }

  /**
   * Find the minimum element of `vector`
   * Notice: it can only be called in th driver.
   */
  def min(vector: PSVectorProxy): Double = {
    assertOnDriver()
    vector.assertValid()
    doMin(vector)
  }

  /**
   * Count the number of non-zero element in `vector`
   * Notice: it can only be called in th driver.
   */
  def nnz(vector: PSVectorProxy): Int = {
    assertOnDriver()
    vector.assertValid()
    doNnz(vector)
  }

  /**
   * Add a `value` to `from` PSVector and save the result to `to` PSVector
   * Notice: it can only be called in th driver.
   */
  def add(from: PSVectorProxy, value: Double, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from.assertValid()
    to.assertValid()
    doAdd(from, value, to)
  }

  /**
   * Subtract a `value` to `from` PSVector and save the result to `to` PSVector
   * Notice: it can only be called in th driver.
   */
  def sub(from: PSVectorProxy, value: Double, to: PSVectorProxy): Unit = {
    add(from, -value, to)
  }

  /**
   * Multiply a `value` to `from` PSVector and save the result to `to` PSVector
   * Notice: it can only be called in th driver.
   */
  def mul(from: PSVectorProxy, value: Double, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from.assertValid()
    to.assertValid()
    doMul(from, value, to)
  }

  /**
   * Divide a `value` to `from` PSVector and save the result to `to` PSVector
   * Notice: it can only be called in th driver.
   */
  def div(from: PSVectorProxy, value: Double, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from.assertValid()
    to.assertValid()
    doDiv(from, value, to)
  }

  /**
   * Corresponding to `scala.math.pow`
   * Notice: it can only be called in th driver.
   */
  def pow(from: PSVectorProxy, value: Double, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from.assertValid()
    to.assertValid()
    doPow(from, value, to)
  }

  /**
   * Corresponding to `scala.math.sqrt`
   * Notice: it can only be called in th driver.
   */
  def sqrt(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from.assertValid()
    to.assertValid()
    doSqrt(from, to)
  }

  /**
   * Corresponding to `scala.math.exp`
   * Notice: it can only be called in th driver.
   */
  def exp(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from.assertValid()
    to.assertValid()
    doExp(from, to)
  }

  /**
   * Corresponding to `scala.math.expm1`
   * Notice: it can only be called in th driver.
   */
  def expm1(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from.assertValid()
    to.assertValid()
    doExpm1(from, to)
  }

  /**
   * Corresponding to `scala.math.log`
   * Notice: it can only be called in th driver.
   */
  def log(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from.assertValid()
    to.assertValid()
    doLog(from, to)
  }

  /**
   * Corresponding to `scala.math.log1p`
   * Notice: it can only be called in th driver.
   */
  def log1p(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from.assertValid()
    to.assertValid()
    doLog1p(from, to)
  }

  /**
   * Corresponding to `scala.math.log10`
   * Notice: it can only be called in th driver.
   */
  def log10(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from.assertValid()
    to.assertValid()
    doLog10(from, to)
  }

  /**
   * Corresponding to `scala.math.ceil`
   * Notice: it can only be called in th driver.
   */
  def ceil(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from.assertValid()
    to.assertValid()
    doCeil(from, to)
  }

  /**
   * Corresponding to `scala.math.floor`
   * Notice: it can only be called in th driver.
   */
  def floor(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from.assertValid()
    to.assertValid()
    doFloor(from, to)
  }

  /**
   * Corresponding to `scala.math.round`
   * Notice: it can only be called in th driver.
   */
  def round(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from.assertValid()
    to.assertValid()
    doRound(from, to)
  }

  /**
   * Corresponding to `scala.math.abs`
   * Notice: it can only be called in th driver.
   */
  def abs(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from.assertValid()
    to.assertValid()
    doAbs(from, to)
  }

  /**
   * Corresponding to `scala.math.signum`
   * Notice: it can only be called in th driver.
   */
  def signum(from: PSVectorProxy, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from.assertValid()
    to.assertValid()
    doSignum(from, to)
  }

  /**
   * Add `from1` PSVector and `from2` PSVector to `to` PSVector
   * Notice: it can only be called in th driver.
   */
  def add(from1: PSVectorProxy, from2: PSVectorProxy, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    from1.assertCompatible(to)
    from2.assertCompatible(to)
    doAdd(from1, from2, to)
  }

  /**
   * Subtract `from2` PSVector from `from1` PSVector and save result to `to` PSVector
   * Notice: it can only be called in th driver.
   */
  def sub(from1: PSVectorProxy, from2: PSVectorProxy, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    from1.assertCompatible(to)
    from2.assertCompatible(to)
    doSub(from1, from2, to)
  }

  /**
   * Multiply `from1` PSVector and `from2` PSVector and save result to `to` PSVector
   * Notice: it can only be called in th driver.
   */
  def mul(from1: PSVectorProxy, from2: PSVectorProxy, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    from1.assertCompatible(to)
    from2.assertCompatible(to)
    doMul(from1, from2, to)
  }

  /**
   * Divide `from1` PSVector to `from2` PSVector and save result to `to` PSVector
   * Notice: it can only be called in th driver.
   */
  def div(from1: PSVectorProxy, from2: PSVectorProxy, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    from1.assertCompatible(to)
    from2.assertCompatible(to)
    doDiv(from1, from2, to)
  }

  /**
   * Find the maximum element of `from1` and `from2` PSVector,
   * and save result to `to` PSVector
   * Notice: it can only be called in th driver.
   */
  def max(from1: PSVectorProxy, from2: PSVectorProxy, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    from1.assertCompatible(to)
    from2.assertCompatible(to)
    doMax(from1, from2, to)
  }

  /**
   * Find the minimum element of `from1` and `from2` PSVector,
   * and save result to `to` PSVector
   * Notice: it can only be called in th driver.
   */
  def min(from1: PSVectorProxy, from2: PSVectorProxy, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    from1.assertCompatible(to)
    from2.assertCompatible(to)
    doMin(from1, from2, to)
  }

  /**
   * Process `MapFunc` for each element of `from` PSVector
   * Notice: it can only be called in th driver.
   */
  def map(from: PSVectorProxy, func: MapFunc, to: PSVectorProxy): Unit = {
    assertOnDriver()
    from.assertValid()
    to.assertValid()
    from.assertCompatible(to)
    doMap(from, func, to)
  }

  /**
   * Process `Zip2MapFunc` for each element of `from1` and `from2` PSVector
   * Notice: it can only be called in th driver.
   */
  def zip2Map(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      func: Zip2MapFunc,
      to: PSVectorProxy): Unit = {
    assertOnDriver()
    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    from1.assertCompatible(to)
    from2.assertCompatible(to)
    doZip2Map(from1, from2, func, to)
  }

  /**
   * Process `Zip3MapFunc` for each element of `from1`,
   * `from2` and `from3` PSVector
   * Notice: it can only be called in th driver.
   */
  def zip3Map(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      from3: PSVectorProxy,
      func: Zip3MapFunc,
      to: PSVectorProxy): Unit = {
    assertOnDriver()
    from1.assertValid()
    from2.assertValid()
    from3.assertValid()
    to.assertValid()
    from1.assertCompatible(to)
    from2.assertCompatible(to)
    from3.assertCompatible(to)
    doZip3Map(from1, from2, from3, func, to)
  }

  /**
   * Process `MapWithIndexFunc` for each element of `from` PSVector
   * Notice: it can only be called in th driver.
   */
  def mapWithIndex(
      from: PSVectorProxy,
      func: MapWithIndexFunc,
      to: PSVectorProxy): Unit = {
    assertOnDriver()
    from.assertValid()
    to.assertValid()
    from.assertCompatible(to)
    doMapWithIndex(from, func, to)
  }

  /**
   * Process `zip2MapWithIndex` for each element of `from1` and `from2` PSVector
   * Notice: it can only be called in th driver.
   */
  def zip2MapWithIndex(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      func: Zip2MapWithIndexFunc,
      to: PSVectorProxy): Unit = {
    assertOnDriver()
    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    from1.assertCompatible(to)
    from2.assertCompatible(to)
    doZip2MapWithIndex(from1, from2, func, to)
  }

  /**
   * Process `Zip3MapWithIndexFunc` for each element of `from1`,
   * `from2` and `from3` PSVector
   * Notice: it can only be called in th driver.
   */
  def zip3MapWithIndex(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      from3: PSVectorProxy,
      func: Zip3MapWithIndexFunc,
      to: PSVectorProxy): Unit = {
    assertOnDriver()
    from1.assertValid()
    from2.assertValid()
    from3.assertValid()
    to.assertValid()
    from1.assertCompatible(to)
    from2.assertCompatible(to)
    from3.assertCompatible(to)
    doZip3MapWithIndex(from1, from2, from3, func, to)
  }

  protected def doCreateVectorPool(
      numDimensions: Int,
      capacity: Int): PSVectorPool

  protected def doDestroyVectorPool(pool: PSVectorPool): Unit

  protected def doPut(
      to: PSVectorProxy,
      value: Array[Double]): Unit

  protected def doFill(
      to: PSVectorProxy,
      value: Double): Unit

  protected def doRandomUniform(
      to: PSVectorProxy,
      min: Double,
      max: Double): Unit

  protected def doRandomNormal(
      to: PSVectorProxy,
      mean: Double,
      stddev: Double): Unit

  protected def doSum(vector: PSVectorProxy): Double

  protected def doMax(vector: PSVectorProxy): Double

  protected def doMin(vector: PSVectorProxy): Double

  protected def doNnz(vector: PSVectorProxy): Int

  protected def doAdd(
      from: PSVectorProxy,
      value: Double,
      to: PSVectorProxy): Unit

  protected def doMul(
      from: PSVectorProxy,
      value: Double,
      to: PSVectorProxy): Unit

  protected def doDiv(
      from: PSVectorProxy,
      value: Double,
      to: PSVectorProxy): Unit

  protected def doPow(
      from: PSVectorProxy,
      value: Double,
      to: PSVectorProxy): Unit

  protected def doSqrt(
      from: PSVectorProxy,
      to: PSVectorProxy): Unit

  protected def doExp(
      from: PSVectorProxy,
      to: PSVectorProxy): Unit

  protected def doExpm1(
      from: PSVectorProxy,
      to: PSVectorProxy): Unit

  protected def doLog(
      from: PSVectorProxy,
      to: PSVectorProxy): Unit

  protected def doLog1p(
      from: PSVectorProxy,
      to: PSVectorProxy): Unit

  protected def doLog10(
      from: PSVectorProxy,
      to: PSVectorProxy): Unit

  protected def doCeil(
      from: PSVectorProxy,
      to: PSVectorProxy): Unit

  protected def doFloor(
      from: PSVectorProxy,
      to: PSVectorProxy): Unit

  protected def doRound(
      from: PSVectorProxy,
      to: PSVectorProxy): Unit

  protected def doAbs(
      from: PSVectorProxy,
      to: PSVectorProxy): Unit

  protected def doSignum(
      from: PSVectorProxy,
      to: PSVectorProxy): Unit

  protected def doAdd(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      to: PSVectorProxy): Unit

  protected def doSub(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      to: PSVectorProxy): Unit

  protected def doMul(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      to: PSVectorProxy): Unit

  protected def doDiv(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      to: PSVectorProxy): Unit

  protected def doMax(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      to: PSVectorProxy): Unit

  protected def doMin(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      to: PSVectorProxy): Unit

  protected def doMap(
      from: PSVectorProxy,
      func: MapFunc,
      to: PSVectorProxy): Unit

  protected def doZip2Map(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      func: Zip2MapFunc,
      to: PSVectorProxy): Unit

  protected def doZip3Map(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      from3: PSVectorProxy,
      func: Zip3MapFunc,
      to: PSVectorProxy): Unit

  protected def doMapWithIndex(
      from: PSVectorProxy,
      func: MapWithIndexFunc,
      to: PSVectorProxy): Unit

  protected def doZip2MapWithIndex(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      func: Zip2MapWithIndexFunc,
      to: PSVectorProxy): Unit

  protected def doZip3MapWithIndex(
      from1: PSVectorProxy,
      from2: PSVectorProxy,
      from3: PSVectorProxy,
      func: Zip3MapWithIndexFunc,
      to: PSVectorProxy): Unit


  /* (driver only) BLAS operators */

  /**
   * Corresponding to `BLAS.axpy`
   * Notice: it can only be called in th driver.
   */
  def axpy(a: Double, x: PSVectorProxy, y: PSVectorProxy): Unit = {
    assertOnDriver()
    x.assertValid()
    y.assertValid()
    x.assertCompatible(y)
    doAxpy(a, x, y)
  }

  /**
   * Corresponding to `BLAS.dot`
   * Notice: it can only be called in th driver.
   */
  def dot(x: PSVectorProxy, y: PSVectorProxy): Double = {
    assertOnDriver()
    x.assertValid()
    y.assertValid()
    x.assertCompatible(y)
    doDot(x, y)
  }

  /**
   * Copy `x` to `y`
   * Notice: it can only be called in th driver.
   */
  def copy(x: PSVectorProxy, y: PSVectorProxy): Unit = {
    assertOnDriver()
    x.assertValid()
    y.assertValid()
    x.assertCompatible(y)
    doCopy(x, y)
  }

  /**
   * Corresponding to `BLAS.scal`
   * Notice: it can only be called in th driver.
   */
  def scal(a: Double, x: PSVectorProxy): Unit = {
    assertOnDriver()
    x.assertValid()
    doScal(a, x)
  }

  /**
   * math.sqrt(x.map(x => x * x).sum)
   * Notice: it can only be called in th driver.
   */
  def nrm2(x: PSVectorProxy): Double = {
    assertOnDriver()
    x.assertValid()
    doNrm2(x)
  }

  /**
   * Calculate the sum of each element absolute value
   * Notice: it can only be called in th driver.
   */
  def asum(x: PSVectorProxy): Double = {
    assertOnDriver()
    x.assertValid()
    doAsum(x)
  }

  /**
   * Find the maximum of each element absolute value
   * Notice: it can only be called in th driver.
   */
  def amax(x: PSVectorProxy): Double = {
    assertOnDriver()
    x.assertValid()
    doAmax(x)
  }

  /**
   * Find the maximum of each element absolute value
   * Notice: it can only be called in th driver.
   */
  def amin(x: PSVectorProxy): Double = {
    assertOnDriver()
    x.assertValid()
    doAmin(x)
  }

  protected def doAxpy(a: Double, x: PSVectorProxy, y: PSVectorProxy): Unit

  protected def doDot(x: PSVectorProxy, y: PSVectorProxy): Double

  protected def doCopy(x: PSVectorProxy, y: PSVectorProxy): Unit

  protected def doScal(a: Double, x: PSVectorProxy): Unit

  protected def doNrm2(x: PSVectorProxy): Double

  protected def doAsum(x: PSVectorProxy): Double

  protected def doAmax(x: PSVectorProxy): Double

  protected def doAmin(x: PSVectorProxy): Double


  /* =========================================== */
  /*          executor only methods              */
  /* =========================================== */

  /**
   * Get the array value of [[PSVectorProxy]]
   */
  def get(vector: PSVectorProxy): Array[Double] = {
    vector.assertValid()
    doGet(vector)
  }

  /**
   * Increment `delta` to `vector`.
   * Notice: only be called in executor
   */
  def increment(vector: PSVectorProxy, delta: Array[Double]): Unit = {
    assertOnExecutor()
    vector.assertValid()
    vector.assertCompatible(delta)
    doIncrement(vector, delta)
  }

  /**
   * Find the maximum number of each dimension.
   * Notice: only be called in executor
   */
  def mergeMax(vector: PSVectorProxy, other: Array[Double]): Unit = {
    assertOnExecutor()
    vector.assertValid()
    vector.assertCompatible(other)
    doMergeMax(vector, other)
  }

  /**
   * Find the minimum number of each dimension.
   * Notice: only be called in executor
   */
  def mergeMin(vector: PSVectorProxy, other: Array[Double]): Unit = {
    assertOnExecutor()
    vector.assertValid()
    vector.assertCompatible(other)
    doMergeMin(vector, other)
  }

  /**
   * Flush the mergeCache to PS nodes.
   * It usually be called after the whole RDD partition elements processed.
   *
   * Notice: only be called in executor
   */
  def flush(): Unit = {
    assertOnExecutor()
    doFlush()
  }

  protected def doGet(vector: PSVectorProxy): Array[Double]

  protected def doIncrement(vector: PSVectorProxy, delta: Array[Double]): Unit

  protected def doMergeMax(vector: PSVectorProxy, other: Array[Double]): Unit

  protected def doMergeMin(vector: PSVectorProxy, other: Array[Double]): Unit

  protected def doFlush(): Unit = {}

  /* =========================================== */
  /*              private methods                */
  /* =========================================== */

  private[spark] def getAngelConf(): Map[String, String]

  private[spark] def register(vector: PSVector): Unit = {}

  protected def getTaskId(): Int = {
    val tc = TaskContext.get()
    if (tc == null) {
      -1
    } else {
      tc.partitionId()
    }
  }

}

object PSClient {

  private[spark] def assertOnDriver(): Unit = {
    if (TaskContext.get() != null) {
      throw new SparkException("Do not call this method on Executors!")
    }
  }

  private[spark] def assertOnExecutor(): Unit = {
    if (TaskContext.get() == null) {
      throw new SparkException("Do not call this method on the Driver!")
    }
  }

  private[spark] def assertOnSpark(): Unit = {
    if (SparkEnv.get == null) {
      throw new SparkException("Create SparkContext first!")
    }
  }

  // The singleton PSClient
  private var _client: PSClient = _

  private var failCause: Exception = _

  /**
   * Setup PSClient after initialize SparkContext.
   * 1. new `_client` instance if it not exists
   * 2. pass angel master configuration to sparkContext for task
   */
  def setup(sc: SparkContext): Unit = {
    assertOnDriver()
    client.getAngelConf().foreach {
      case (key, value) => sc.setLocalProperty(key, value)
    }
  }

  /**
   * Clean up PSClient.
   */
  def cleanup(sc: SparkContext): Unit = {
    if (isAngelMode(sc.getConf)) {
      AngelPSClient.stop()
    }
    _client = null
  }

  /**
   * Get the PSClient instance
   * new `_client` instance if it not exists
   */
  def get: PSClient = {
    if (client == null) {
      throw new SparkException("PSClient init failed!", failCause)
    }
    client
  }

  /**
   * new `_client` singleton instance if it not exists
   * @return
   */
  private def client: PSClient = {
    if (_client == null) {
      try {
        assertOnSpark()
        val env = SparkEnv.get
        if (isAngelMode(env.conf)) {
          _client = AngelPSClient(env.executorId, env.conf)
        } else {
          _client = new LocalPSClient
        }
      } catch {
        case e: Exception =>
          _client = null
          failCause = e
      }
    }
    _client
  }

  private def isLocalMaster(conf: SparkConf): Boolean = {
    val master = conf.get("spark.master", "")
    master.toLowerCase.startsWith("local")
  }

  /**
   * For AngelPSClient, figure out PS Mode is LOCAL or YARN
   */
  private def isAngelMode(conf: SparkConf): Boolean = {
    val psMode = conf.getOption("spark.ps.mode")
    var isAngelClient = false
    if (!isLocalMaster(conf)) {
      isAngelClient = true
    } else {
      if (psMode.isDefined &&
        psMode.get == AngelDeployMode.LOCAL.toString) {
        isAngelClient = true
      }
    }
    isAngelClient
  }
}
