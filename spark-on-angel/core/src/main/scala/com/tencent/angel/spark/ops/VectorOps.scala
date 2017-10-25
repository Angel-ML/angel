package com.tencent.angel.spark.ops

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math.vector.DenseDoubleVector
import com.tencent.angel.ml.matrix.psf.aggr._
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarAggrResult
import com.tencent.angel.ml.matrix.psf.aggr.primitive.Pull
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.get.single.GetRowResult
import com.tencent.angel.ml.matrix.psf.update._
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc
import com.tencent.angel.ml.matrix.psf.update.enhance.map._
import com.tencent.angel.ml.matrix.psf.update.enhance.zip2.{Zip2Map, Zip2MapFunc, Zip2MapWithIndex, Zip2MapWithIndexFunc}
import com.tencent.angel.ml.matrix.psf.update.enhance.zip3.{Zip3Map, Zip3MapFunc, Zip3MapWithIndex, Zip3MapWithIndexFunc}
import com.tencent.angel.ml.matrix.psf.update.primitive.{Increment, Push}
import com.tencent.angel.psagent.matrix.{MatrixClientFactory, ResponseType, Result}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.matrix.PSMatrix
import com.tencent.angel.spark.models.vector.PSVector
import org.apache.commons.logging.LogFactory

class VectorOps {
  val LOG = LogFactory.getLog(classOf[VectorOps])
  /**
    * Put `value` to PSVectorKey
    * Notice: it can only be called in th driver.
    */
  def push(to: PSVector, value: Array[Double]): Unit = {
    to.assertValid()
    to.assertCompatible(value)
    update(to.poolId, new Push(to.poolId, to.id, value))
  }

  /**
    * Get the array value of [[PSVector]]
    */
  def pull(vector: PSVector): Array[Double] = {
    vector.assertValid()
    doPull(vector)
  }


  /**
    * Process `MapFunc` for each element of `from` PSVector
    * Notice: it can only be called in th driver.
    */
  def map(from: PSVector, func: MapFunc, to: PSVector): Unit = {
    from.assertValid()
    to.assertValid()
    from.assertCompatible(to)
    doMap(from, func, to)
  }

  def mapInPlace(proxy: PSVector, func: MapFunc): Unit = {
    proxy.assertValid()
    doMapInPlace(proxy, func)
  }

  /**
    * Process `Zip2MapFunc` for each element of `from1` and `from2` PSVector
    * Notice: it can only be called in th driver.
    */
  def zip2Map(
               from1: PSVector,
               from2: PSVector,
               func: Zip2MapFunc,
               to: PSVector): Unit = {

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
               from1: PSVector,
               from2: PSVector,
               from3: PSVector,
               func: Zip3MapFunc,
               to: PSVector): Unit = {

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
                    from: PSVector,
                    func: MapWithIndexFunc,
                    to: PSVector): Unit = {

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
                        from1: PSVector,
                        from2: PSVector,
                        func: Zip2MapWithIndexFunc,
                        to: PSVector): Unit = {

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
                        from1: PSVector,
                        from2: PSVector,
                        from3: PSVector,
                        func: Zip3MapWithIndexFunc,
                        to: PSVector): Unit = {

    from1.assertValid()
    from2.assertValid()
    from3.assertValid()
    to.assertValid()
    from1.assertCompatible(to)
    from2.assertCompatible(to)
    from3.assertCompatible(to)
    doZip3MapWithIndex(from1, from2, from3, func, to)
  }

  /**
    * Judge if v1 is equal to v2 by element-wise.
    */
  def equal(v1: PSVector, v2: PSVector): Boolean = {
    v1.assertCompatible(v2)
    doEqual(v1, v2)
  }


  /**
    * Sum all the dimension of `vector`
    * Notice: it can only be called in th driver.
    */
  def sum(vector: PSVector): Double = {
    vector.assertValid()
    doSum(vector)
  }

  /**
    * Find the maximum element of `vector`
    * Notice: it can only be called in th driver.
    */
  def max(vector: PSVector): Double = {
    vector.assertValid()
    doMax(vector)
  }

  /**
    * Find the minimum element of `vector`
    * Notice: it can only be called in th driver.
    */
  def min(vector: PSVector): Double = {
    vector.assertValid()
    doMin(vector)
  }

  /**
    * Count the number of non-zero element in `vector`
    * Notice: it can only be called in th driver.
    */
  def nnz(vector: PSVector): Int = {
    vector.assertValid()
    doNnz(vector)
  }

  /**
    * Add a `value` to `from` PSVector and save the result to `to` PSVector
    * Notice: it can only be called in th driver.
    */
  def add(from: PSVector, value: Double, to: PSVector): Unit = {
    from.assertValid()
    to.assertValid()
    doAdd(from, value, to)
  }

  /**
    * Subtract a `value` to `from` PSVector and save the result to `to` PSVector
    * Notice: it can only be called in th driver.
    */
  def sub(from: PSVector, value: Double, to: PSVector): Unit = {
    add(from, -value, to)
  }

  /**
    * Multiply a `value` to `from` PSVector and save the result to `to` PSVector
    * Notice: it can only be called in th driver.
    */
  def mul(from: PSVector, value: Double, to: PSVector): Unit = {

    from.assertValid()
    to.assertValid()
    doMul(from, value, to)
  }

  /**
    * Divide a `value` to `from` PSVector and save the result to `to` PSVector
    * Notice: it can only be called in th driver.
    */
  def div(from: PSVector, value: Double, to: PSVector): Unit = {

    from.assertValid()
    to.assertValid()
    doDiv(from, value, to)
  }

  /**
    * Corresponding to `scala.math.pow`
    * Notice: it can only be called in th driver.
    */
  def pow(from: PSVector, value: Double, to: PSVector): Unit = {

    from.assertValid()
    to.assertValid()
    doPow(from, value, to)
  }

  /**
    * Corresponding to `scala.math.sqrt`
    * Notice: it can only be called in th driver.
    */
  def sqrt(from: PSVector, to: PSVector): Unit = {

    from.assertValid()
    to.assertValid()
    doSqrt(from, to)
  }

  /**
    * Corresponding to `scala.math.exp`
    * Notice: it can only be called in th driver.
    */
  def exp(from: PSVector, to: PSVector): Unit = {

    from.assertValid()
    to.assertValid()
    doExp(from, to)
  }

  /**
    * Corresponding to `scala.math.expm1`
    * Notice: it can only be called in th driver.
    */
  def expm1(from: PSVector, to: PSVector): Unit = {

    from.assertValid()
    to.assertValid()
    doExpm1(from, to)
  }

  /**
    * Corresponding to `scala.math.log`
    * Notice: it can only be called in th driver.
    */
  def log(from: PSVector, to: PSVector): Unit = {

    from.assertValid()
    to.assertValid()
    doLog(from, to)
  }

  /**
    * Corresponding to `scala.math.log1p`
    * Notice: it can only be called in th driver.
    */
  def log1p(from: PSVector, to: PSVector): Unit = {

    from.assertValid()
    to.assertValid()
    doLog1p(from, to)
  }

  /**
    * Corresponding to `scala.math.log10`
    * Notice: it can only be called in th driver.
    */
  def log10(from: PSVector, to: PSVector): Unit = {

    from.assertValid()
    to.assertValid()
    doLog10(from, to)
  }

  /**
    * Corresponding to `scala.math.ceil`
    * Notice: it can only be called in th driver.
    */
  def ceil(from: PSVector, to: PSVector): Unit = {

    from.assertValid()
    to.assertValid()
    doCeil(from, to)
  }

  /**
    * Corresponding to `scala.math.floor`
    * Notice: it can only be called in th driver.
    */
  def floor(from: PSVector, to: PSVector): Unit = {

    from.assertValid()
    to.assertValid()
    doFloor(from, to)
  }

  /**
    * Corresponding to `scala.math.round`
    * Notice: it can only be called in th driver.
    */
  def round(from: PSVector, to: PSVector): Unit = {

    from.assertValid()
    to.assertValid()
    doRound(from, to)
  }

  /**
    * Corresponding to `scala.math.abs`
    * Notice: it can only be called in th driver.
    */
  def abs(from: PSVector, to: PSVector): Unit = {

    from.assertValid()
    to.assertValid()
    doAbs(from, to)
  }

  /**
    * Corresponding to `scala.math.signum`
    * Notice: it can only be called in th driver.
    */
  def signum(from: PSVector, to: PSVector): Unit = {

    from.assertValid()
    to.assertValid()
    doSignum(from, to)
  }

  /**
    * Add `from1` PSVector and `from2` PSVector to `to` PSVector
    * Notice: it can only be called in th driver.
    */
  def add(from1: PSVector, from2: PSVector, to: PSVector): Unit = {

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
  def sub(from1: PSVector, from2: PSVector, to: PSVector): Unit = {

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
  def mul(from1: PSVector, from2: PSVector, to: PSVector): Unit = {

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
  def div(from1: PSVector, from2: PSVector, to: PSVector): Unit = {

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
  def max(from1: PSVector, from2: PSVector, to: PSVector): Unit = {

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
  def min(from1: PSVector, from2: PSVector, to: PSVector): Unit = {

    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    from1.assertCompatible(to)
    from2.assertCompatible(to)
    doMin(from1, from2, to)
  }


  def doSum(vector: PSVector): Double = {
    aggregate(vector.poolId, new Sum(vector.poolId, vector.id)).asInstanceOf[ScalarAggrResult].getResult
  }

  def doMax(vector: PSVector): Double = {
    aggregate(vector.poolId, new Max(vector.poolId, vector.id)).asInstanceOf[ScalarAggrResult].getResult
  }

  def doMin(vector: PSVector): Double = {
    aggregate(vector.poolId, new Min(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  def doNnz(vector: PSVector): Int = {
    aggregate(vector.poolId, new Nnz(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult.toInt
  }

  def doAdd(from: PSVector, value: Double, to: PSVector): Unit = {
    update(from.poolId, new AddS(from.poolId, from.id, to.id, value))
  }

  def doMul(from: PSVector, value: Double, to: PSVector): Unit = {
    update(from.poolId, new MulS(from.poolId, from.id, to.id, value))
  }

  def doDiv(from: PSVector, value: Double, to: PSVector): Unit = {
    update(from.poolId, new DivS(from.poolId, from.id, to.id, value))
  }

  def doPow(from: PSVector, value: Double, to: PSVector): Unit = {
    update(from.poolId, new Pow(from.poolId, from.id, to.id, value))
  }

  def doSqrt(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Sqrt(from.poolId, from.id, to.id))
  }

  def doExp(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Exp(from.poolId, from.id, to.id))
  }

  def doExpm1(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Expm1(from.poolId, from.id, to.id))
  }

  def doLog(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Log(from.poolId, from.id, to.id))
  }

  def doLog1p(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Log1p(from.poolId, from.id, to.id))
  }

  def doLog10(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Log10(from.poolId, from.id, to.id))
  }

  def doCeil(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Ceil(from.poolId, from.id, to.id))
  }

  def doFloor(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Floor(from.poolId, from.id, to.id))
  }

  def doRound(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Round(from.poolId, from.id, to.id))
  }

  def doAbs(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Abs(from.poolId, from.id, to.id))
  }

  def doSignum(from: PSVector, to: PSVector): Unit = {
    update(from.poolId, new Signum(from.poolId, from.id, to.id))
  }

  def doAdd(
             from1: PSVector,
             from2: PSVector,
             to: PSVector): Unit = {
    update(from1.poolId, new Add(from1.poolId, from1.id, from2.id, to.id))
  }

  def doSub(
             from1: PSVector,
             from2: PSVector,
             to: PSVector): Unit = {
    update(from1.poolId, new Sub(from1.poolId, from1.id, from2.id, to.id))
  }

  def doMul(
             from1: PSVector,
             from2: PSVector,
             to: PSVector): Unit = {
    update(from1.poolId, new Mul(from1.poolId, from1.id, from2.id, to.id))
  }

  def doDiv(
             from1: PSVector,
             from2: PSVector,
             to: PSVector): Unit = {
    update(from1.poolId, new Div(from1.poolId, from1.id, from2.id, to.id))
  }

  def doMax(
             from1: PSVector,
             from2: PSVector,
             to: PSVector): Unit = {
    update(from1.poolId, new MaxV(from1.poolId, from1.id, from2.id, to.id))
  }

  def doMin(
             from1: PSVector,
             from2: PSVector,
             to: PSVector): Unit = {
    update(from1.poolId, new MinV(from1.poolId, from1.id, from2.id, to.id))
  }

  def doMap(from: PSVector,
            func: MapFunc,
            to: PSVector): Unit = {
    update(from.poolId, new Map(from.poolId, from.id, to.id, func))
  }

  def doMapInPlace(proxy: PSVector, func: MapFunc): Unit = {
    update(proxy.poolId, new MapInPlace(proxy.poolId, proxy.id, func))
  }

  def doZip2Map(
                 from1: PSVector,
                 from2: PSVector,
                 func: Zip2MapFunc,
                 to: PSVector): Unit = {
    update(from1.poolId, new Zip2Map(from1.poolId, from1.id, from2.id, to.id, func))
  }

  def doZip3Map(
                 from1: PSVector,
                 from2: PSVector,
                 from3: PSVector,
                 func: Zip3MapFunc,
                 to: PSVector): Unit = {
    update(from1.poolId,
      new Zip3Map(from1.poolId, from1.id, from2.id, from3.id, to.id, func))
  }

  def doMapWithIndex(
                      from: PSVector,
                      func: MapWithIndexFunc,
                      to: PSVector): Unit = {
    update(from.poolId, new MapWithIndex(from.poolId, from.id, to.id, func))
  }

  def doZip2MapWithIndex(
                          from1: PSVector,
                          from2: PSVector,
                          func: Zip2MapWithIndexFunc,
                          to: PSVector): Unit = {
    update(from1.poolId,
      new Zip2MapWithIndex(from1.poolId, from1.id, from2.id, to.id, func))
  }

  def doZip3MapWithIndex(
                          from1: PSVector,
                          from2: PSVector,
                          from3: PSVector,
                          func: Zip3MapWithIndexFunc,
                          to: PSVector): Unit = {
    update(from1.poolId,
      new Zip3MapWithIndex(from1.poolId, from1.id, from2.id, from3.id, to.id, func))
  }

  def doAxpy(a: Double, x: PSVector, y: PSVector): Unit = {
    update(x.poolId, new Axpy(x.poolId, x.id, y.id, a))
  }

  def doDot(x: PSVector, y: PSVector): Double = {
    aggregate(x.poolId, new Dot(x.poolId, x.id, y.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  def doCopy(x: PSVector, y: PSVector): Unit = {
    update(x.poolId, new Copy(x.poolId, x.id, y.id))
  }

  def doScal(a: Double, x: PSVector): Unit = {
    update(x.poolId, new Scale(x.poolId, x.id, a))
  }

  def doNrm2(x: PSVector): Double = {
    aggregate(x.poolId, new Nrm2(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  def doAsum(x: PSVector): Double = {
    aggregate(x.poolId, new Asum(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  def doAmax(x: PSVector): Double = {
    aggregate(x.poolId, new Amax(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  def doAmin(x: PSVector): Double = {
    aggregate(x.poolId, new Amin(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  def doIncrement(vector: PSVector, delta: Array[Double]): Unit = {
    update(vector.poolId, new Increment(vector.poolId, vector.id, delta))
  }

  def doMergeMax(vector: PSVector, other: Array[Double]): Unit = {
    update(vector.poolId, new MaxA(vector.poolId, vector.id, other))
  }

  def doMergeMin(vector: PSVector, other: Array[Double]): Unit = {
    update(vector.poolId, new MinA(vector.poolId, vector.id, other))
  }

  /* (driver only) BLAS operators */

  /**
    * Corresponding to `BLAS.axpy`
    * Notice: it can only be called in th driver.
    */
  def axpy(a: Double, x: PSVector, y: PSVector): Unit = {

    x.assertValid()
    y.assertValid()
    x.assertCompatible(y)
    doAxpy(a, x, y)
  }

  /**
    * Corresponding to `BLAS.dot`
    * Notice: it can only be called in th driver.
    */
  def dot(x: PSVector, y: PSVector): Double = {

    x.assertValid()
    y.assertValid()
    x.assertCompatible(y)
    doDot(x, y)
  }

  /**
    * Copy `x` to `y`
    * Notice: it can only be called in th driver.
    */
  def copy(x: PSVector, y: PSVector): Unit = {
    x.assertValid()
    y.assertValid()
    x.assertCompatible(y)
    doCopy(x, y)
  }

  /**
    * Corresponding to `BLAS.scal`
    * Notice: it can only be called in th driver.
    */
  def scal(a: Double, x: PSVector): Unit = {
    x.assertValid()
    doScal(a, x)
  }

  /**
    * math.sqrt(x.map(x => x * x).sum)
    * Notice: it can only be called in th driver.
    */
  def nrm2(x: PSVector): Double = {

    x.assertValid()
    doNrm2(x)
  }

  /**
    * Calculate the sum of each element absolute value
    * Notice: it can only be called in th driver.
    */
  def asum(x: PSVector): Double = {

    x.assertValid()
    doAsum(x)
  }

  /**
    * Find the maximum of each element absolute value
    * Notice: it can only be called in th driver.
    */
  def amax(x: PSVector): Double = {

    x.assertValid()
    doAmax(x)
  }

  /**
    * Find the maximum of each element absolute value
    * Notice: it can only be called in th driver.
    */
  def amin(x: PSVector): Double = {

    x.assertValid()
    doAmin(x)
  }


  /* =========================================== */
  /*          executor only methods              */
  /* =========================================== */

  /**
    * Increment `delta` to `vector`.
    * Notice: only be called in executor
    */
  def increment(vector: PSVector, delta: Array[Double]): Unit = {

    vector.assertValid()
    vector.assertCompatible(delta)
    doIncrement(vector, delta)
  }

  /**
    * Find the maximum number of each dimension.
    * Notice: only be called in executor
    */
  def mergeMax(vector: PSVector, other: Array[Double]): Unit = {

    vector.assertValid()
    vector.assertCompatible(other)
    doMergeMax(vector, other)
  }

  /**
    * Find the minimum number of each dimension.
    * Notice: only be called in executor
    */
  def mergeMin(vector: PSVector, other: Array[Double]): Unit = {
    vector.assertValid()
    vector.assertCompatible(other)
    doMergeMin(vector, other)
  }

  private def update(modelId: Int, func: UpdateFunc): Unit = {
    val client = MatrixClientFactory.get(modelId, PSContext.getTaskId())
    val result = client.update(func).get
    assertSuccess(result)
  }

  private def aggregate(matrix: PSMatrix, func: GetFunc): GetResult = {
    val client = MatrixClientFactory.get(matrix.meta.getId, PSContext.getTaskId())
    val result = client.get(func)
    assertSuccess(result)
    result
  }


  private def assertSuccess(result: Result): Unit = {
    if (result.getResponseType == ResponseType.FAILED) {
      throw new AngelException("PS computation failed!")
    }
  }


  /**
    * Use ps Function(PSF) to aggregate a PSVector on PS nodes.
    */
  private[spark] def aggregate(modelId: Int, func: GetFunc): GetResult = {
    LOG.info("before call get psf, model id=" + modelId + ", taskId=" + PSContext.getTaskId())
    val client = MatrixClientFactory.get(modelId, PSContext.getTaskId())
    val result = client.get(func)
    assertSuccess(result)
    result
  }


  def doPull(vector: PSVector): Array[Double] = {
    aggregate(vector.poolId, new Pull(vector.poolId, vector.id)).asInstanceOf[GetRowResult].getRow.asInstanceOf[DenseDoubleVector].getValues
  }

  def doEqual(vector1: PSVector, vector2: PSVector): Boolean = {
    val res = aggregate(vector1.poolId, new Equal(vector1.poolId, vector1.id, vector2.id)).asInstanceOf[ScalarAggrResult].getResult
    res == 1.0
  }

}
