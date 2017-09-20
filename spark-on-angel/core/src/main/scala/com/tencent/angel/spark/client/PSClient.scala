/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.spark.client

import com.github.fommil.netlib.F2jBLAS

import com.tencent.angel.ml.matrix.psf.update.enhance.map.{MapFunc, MapWithIndexFunc}
import com.tencent.angel.ml.matrix.psf.update.enhance.zip2.{Zip2MapFunc, Zip2MapWithIndexFunc}
import com.tencent.angel.ml.matrix.psf.update.enhance.zip3.{Zip3MapFunc, Zip3MapWithIndexFunc}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.math.matrix.PSMatrix
import com.tencent.angel.spark.math.vector.PSVector

/**
 * PSClient is a _instance which contains operations for PSVector on the PS nodes.
 * These operations can be called on the Spark driver or executor.
 */
private[spark] abstract class PSClient {

  private[spark] val BLAS = new F2jBLAS

  /* =========================================== */
  /*          driver only methods                */
  /* =========================================== */

  /**
   * Put `value` to PSVectorKey
   * Notice: it can only be called in th driver.
   */
  def push(to: PSVector, value: Array[Double]): Unit = {
    to.assertValid()
    to.assertCompatible(value)
    doPush(to, value)
  }

  /**
   * Get the array value of [[PSVector]]
   */
  def pull(vector: PSVector): Array[Double] = {
    vector.assertValid()
    doPull(vector)
  }

  /**
   * Fill PSVectorKey with `value`
   * Notice: it can only be called in th driver.
   */
  def fill(to: PSVector, value: Double): Unit = {
    to.assertValid()
    doFill(to, value)
  }

  def fill(to: PSVector, values: Array[Double]): Unit = {
    to.assertValid()
    doPush(to, values)
  }

  /**
   * Generate a random PSVector, the random distribution is uniform.
   * Notice: it can only be called in th driver.
   *
   * @param min the minimum of uniform distribution
   * @param max the maximum of uniform distribution
   */
  def randomUniform(to: PSVector, min: Double, max: Double): Unit = {
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
  def randomNormal(to: PSVector, mean: Double, stddev: Double): Unit = {
    to.assertValid()
    doRandomNormal(to, mean, stddev)
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

  protected def doPull(vector: PSVector): Array[Double]

  protected def doPush(vector: PSVector, array: Array[Double]): Unit

  protected def doFill(
                        to: PSVector,
                        value: Double): Unit

  protected def doRandomUniform(
                                 to: PSVector,
                                 min: Double,
                                 max: Double): Unit

  protected def doRandomNormal(
                                to: PSVector,
                                mean: Double,
                                stddev: Double): Unit

  protected def doEqual(v1: PSVector, v2: PSVector): Boolean

  protected def doSum(vector: PSVector): Double

  protected def doMax(vector: PSVector): Double

  protected def doMin(vector: PSVector): Double

  protected def doNnz(vector: PSVector): Int

  protected def doAdd(
                       from: PSVector,
                       value: Double,
                       to: PSVector): Unit

  protected def doMul(
                       from: PSVector,
                       value: Double,
                       to: PSVector): Unit

  protected def doDiv(
                       from: PSVector,
                       value: Double,
                       to: PSVector): Unit

  protected def doPow(
                       from: PSVector,
                       value: Double,
                       to: PSVector): Unit

  protected def doSqrt(
                        from: PSVector,
                        to: PSVector): Unit

  protected def doExp(
                       from: PSVector,
                       to: PSVector): Unit

  protected def doExpm1(
                         from: PSVector,
                         to: PSVector): Unit

  protected def doLog(
                       from: PSVector,
                       to: PSVector): Unit

  protected def doLog1p(
                         from: PSVector,
                         to: PSVector): Unit

  protected def doLog10(
                         from: PSVector,
                         to: PSVector): Unit

  protected def doCeil(
                        from: PSVector,
                        to: PSVector): Unit

  protected def doFloor(
                         from: PSVector,
                         to: PSVector): Unit

  protected def doRound(
                         from: PSVector,
                         to: PSVector): Unit

  protected def doAbs(
                       from: PSVector,
                       to: PSVector): Unit

  protected def doSignum(
                          from: PSVector,
                          to: PSVector): Unit

  protected def doAdd(
                       from1: PSVector,
                       from2: PSVector,
                       to: PSVector): Unit

  protected def doSub(
                       from1: PSVector,
                       from2: PSVector,
                       to: PSVector): Unit

  protected def doMul(
                       from1: PSVector,
                       from2: PSVector,
                       to: PSVector): Unit

  protected def doDiv(
                       from1: PSVector,
                       from2: PSVector,
                       to: PSVector): Unit

  protected def doMax(
                       from1: PSVector,
                       from2: PSVector,
                       to: PSVector): Unit

  protected def doMin(
                       from1: PSVector,
                       from2: PSVector,
                       to: PSVector): Unit

  protected def doMap(
                       from: PSVector,
                       func: MapFunc,
                       to: PSVector): Unit

  protected def doMapInPlace(proxy: PSVector, func: MapFunc): Unit

  protected def doZip2Map(
                           from1: PSVector,
                           from2: PSVector,
                           func: Zip2MapFunc,
                           to: PSVector): Unit

  protected def doZip3Map(
                           from1: PSVector,
                           from2: PSVector,
                           from3: PSVector,
                           func: Zip3MapFunc,
                           to: PSVector): Unit

  protected def doMapWithIndex(
                                from: PSVector,
                                func: MapWithIndexFunc,
                                to: PSVector): Unit

  protected def doZip2MapWithIndex(
                                    from1: PSVector,
                                    from2: PSVector,
                                    func: Zip2MapWithIndexFunc,
                                    to: PSVector): Unit

  protected def doZip3MapWithIndex(
                                    from1: PSVector,
                                    from2: PSVector,
                                    from3: PSVector,
                                    func: Zip3MapWithIndexFunc,
                                    to: PSVector): Unit


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

  protected def doAxpy(a: Double, x: PSVector, y: PSVector): Unit

  protected def doDot(x: PSVector, y: PSVector): Double

  protected def doCopy(x: PSVector, y: PSVector): Unit

  protected def doScal(a: Double, x: PSVector): Unit

  protected def doNrm2(x: PSVector): Double

  protected def doAsum(x: PSVector): Double

  protected def doAmax(x: PSVector): Double

  protected def doAmin(x: PSVector): Double


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

  protected def doIncrement(vector: PSVector, delta: Array[Double]): Unit

  protected def doMergeMax(vector: PSVector, other: Array[Double]): Unit

  protected def doMergeMin(vector: PSVector, other: Array[Double]): Unit


  /**
   * ===================================================
   * The following methods are matrix oriented.
   * ===================================================
   */

  /**
   * Pull matrix to local
   */
  def pull(mat: PSMatrix): Array[Array[Double]] = {
    mat.assertValid()
    doPull(mat)
  }

  /**
   * Initialize a random matrix, whose value is a random(0.0, 1.0)
   */
  def random(mat: PSMatrix): Unit = {
    mat.assertValid()
    doRandom(mat)
  }

  /**
   * Assign matrix diagonal with `value`
   */
  def diag(mat: PSMatrix, value: Array[Double]): Unit = {
    mat.assertValid()
    mat.assertCompatible(value)
    assert(mat.meta.getColNum == mat.meta.getRowNum, s"when init diag matrix, " +
      s"matrix columnNum(${mat.meta.getColNum}) must equal to rowNum(${mat.meta.getRowNum})")
    doDiag(mat, value)
  }

  /**
   * Assign matrix diagonal with 1.0
   */
  def eye(mat: PSMatrix): Unit = {
    mat.assertValid()
    assert(mat.meta.getColNum == mat.meta.getRowNum, s"when init eye matrix, " +
      s"matrix columnNum(${mat.meta.getColNum}) must equal to rowNum(${mat.meta.getRowNum})")
    doEye(mat)
  }

  /**
   * Fill matrix with `value`
   */
  def fill(mat: PSMatrix, value: Double): Unit = {
    mat.assertValid()
    doFill(mat, value)
  }

  protected def doPull(matrix: PSMatrix): Array[Array[Double]]

  protected def doRandom(mat: PSMatrix): Unit

  protected def doDiag(mat: PSMatrix, value: Array[Double]): Unit

  protected def doEye(mat: PSMatrix): Unit

  protected def doFill(mat: PSMatrix, value: Double): Unit
}

object PSClient {
  private var _instance: PSClient = _

  def instance(): PSClient = {
    if (_instance == null) {
      classOf[PSClient].synchronized {
        if (_instance == null) {
          _instance = new AngelPSClient(PSContext.instance())
        }
      }
    }
    _instance
  }
}
