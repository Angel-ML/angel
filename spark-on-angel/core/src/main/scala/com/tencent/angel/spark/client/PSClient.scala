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
import com.tencent.angel.spark.model.PSModelProxy
import com.tencent.angel.spark.model.matrix.PSMatrix

/**
 * PSClient is a client which contains operations for PSVector on the PS nodes.
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
  def push(to: PSModelProxy, value: Array[Double]): Unit = {
    to.assertValid()
    to.assertCompatible(value)
    doPush(to, value)
  }

  /**
   * Get the array value of [[PSModelProxy]]
   */
  def pull(vector: PSModelProxy): Array[Double] = {
    vector.assertValid()
    doPull(vector)
  }

  /**
   * Fill PSVectorKey with `value`
   * Notice: it can only be called in th driver.
   */
  def fill(to: PSModelProxy, value: Double): Unit = {
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
  def randomUniform(to: PSModelProxy, min: Double, max: Double): Unit = {
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
  def randomNormal(to: PSModelProxy, mean: Double, stddev: Double): Unit = {
    to.assertValid()
    doRandomNormal(to, mean, stddev)
  }

  /**
   * Judge if v1 is equal to v2 by element-wise.
   */
  def equal(v1: PSModelProxy, v2: PSModelProxy): Boolean = {
    v1.assertCompatible(v2)
    doEqual(v1, v2)
  }

  /**
   * Sum all the dimension of `vector`
   * Notice: it can only be called in th driver.
   */
  def sum(vector: PSModelProxy): Double = {
    vector.assertValid()
    doSum(vector)
  }

  /**
   * Find the maximum element of `vector`
   * Notice: it can only be called in th driver.
   */
  def max(vector: PSModelProxy): Double = {
    vector.assertValid()
    doMax(vector)
  }

  /**
   * Find the minimum element of `vector`
   * Notice: it can only be called in th driver.
   */
  def min(vector: PSModelProxy): Double = {
    vector.assertValid()
    doMin(vector)
  }

  /**
   * Count the number of non-zero element in `vector`
   * Notice: it can only be called in th driver.
   */
  def nnz(vector: PSModelProxy): Int = {
    vector.assertValid()
    doNnz(vector)
  }

  /**
   * Add a `value` to `from` PSVector and save the result to `to` PSVector
   * Notice: it can only be called in th driver.
   */
  def add(from: PSModelProxy, value: Double, to: PSModelProxy): Unit = {
    from.assertValid()
    to.assertValid()
    doAdd(from, value, to)
  }

  /**
   * Subtract a `value` to `from` PSVector and save the result to `to` PSVector
   * Notice: it can only be called in th driver.
   */
  def sub(from: PSModelProxy, value: Double, to: PSModelProxy): Unit = {
    add(from, -value, to)
  }

  /**
   * Multiply a `value` to `from` PSVector and save the result to `to` PSVector
   * Notice: it can only be called in th driver.
   */
  def mul(from: PSModelProxy, value: Double, to: PSModelProxy): Unit = {

    from.assertValid()
    to.assertValid()
    doMul(from, value, to)
  }

  /**
   * Divide a `value` to `from` PSVector and save the result to `to` PSVector
   * Notice: it can only be called in th driver.
   */
  def div(from: PSModelProxy, value: Double, to: PSModelProxy): Unit = {

    from.assertValid()
    to.assertValid()
    doDiv(from, value, to)
  }

  /**
   * Corresponding to `scala.math.pow`
   * Notice: it can only be called in th driver.
   */
  def pow(from: PSModelProxy, value: Double, to: PSModelProxy): Unit = {

    from.assertValid()
    to.assertValid()
    doPow(from, value, to)
  }

  /**
   * Corresponding to `scala.math.sqrt`
   * Notice: it can only be called in th driver.
   */
  def sqrt(from: PSModelProxy, to: PSModelProxy): Unit = {

    from.assertValid()
    to.assertValid()
    doSqrt(from, to)
  }

  /**
   * Corresponding to `scala.math.exp`
   * Notice: it can only be called in th driver.
   */
  def exp(from: PSModelProxy, to: PSModelProxy): Unit = {

    from.assertValid()
    to.assertValid()
    doExp(from, to)
  }

  /**
   * Corresponding to `scala.math.expm1`
   * Notice: it can only be called in th driver.
   */
  def expm1(from: PSModelProxy, to: PSModelProxy): Unit = {

    from.assertValid()
    to.assertValid()
    doExpm1(from, to)
  }

  /**
   * Corresponding to `scala.math.log`
   * Notice: it can only be called in th driver.
   */
  def log(from: PSModelProxy, to: PSModelProxy): Unit = {

    from.assertValid()
    to.assertValid()
    doLog(from, to)
  }

  /**
   * Corresponding to `scala.math.log1p`
   * Notice: it can only be called in th driver.
   */
  def log1p(from: PSModelProxy, to: PSModelProxy): Unit = {

    from.assertValid()
    to.assertValid()
    doLog1p(from, to)
  }

  /**
   * Corresponding to `scala.math.log10`
   * Notice: it can only be called in th driver.
   */
  def log10(from: PSModelProxy, to: PSModelProxy): Unit = {

    from.assertValid()
    to.assertValid()
    doLog10(from, to)
  }

  /**
   * Corresponding to `scala.math.ceil`
   * Notice: it can only be called in th driver.
   */
  def ceil(from: PSModelProxy, to: PSModelProxy): Unit = {

    from.assertValid()
    to.assertValid()
    doCeil(from, to)
  }

  /**
   * Corresponding to `scala.math.floor`
   * Notice: it can only be called in th driver.
   */
  def floor(from: PSModelProxy, to: PSModelProxy): Unit = {

    from.assertValid()
    to.assertValid()
    doFloor(from, to)
  }

  /**
   * Corresponding to `scala.math.round`
   * Notice: it can only be called in th driver.
   */
  def round(from: PSModelProxy, to: PSModelProxy): Unit = {

    from.assertValid()
    to.assertValid()
    doRound(from, to)
  }

  /**
   * Corresponding to `scala.math.abs`
   * Notice: it can only be called in th driver.
   */
  def abs(from: PSModelProxy, to: PSModelProxy): Unit = {

    from.assertValid()
    to.assertValid()
    doAbs(from, to)
  }

  /**
   * Corresponding to `scala.math.signum`
   * Notice: it can only be called in th driver.
   */
  def signum(from: PSModelProxy, to: PSModelProxy): Unit = {

    from.assertValid()
    to.assertValid()
    doSignum(from, to)
  }

  /**
   * Add `from1` PSVector and `from2` PSVector to `to` PSVector
   * Notice: it can only be called in th driver.
   */
  def add(from1: PSModelProxy, from2: PSModelProxy, to: PSModelProxy): Unit = {

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
  def sub(from1: PSModelProxy, from2: PSModelProxy, to: PSModelProxy): Unit = {

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
  def mul(from1: PSModelProxy, from2: PSModelProxy, to: PSModelProxy): Unit = {

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
  def div(from1: PSModelProxy, from2: PSModelProxy, to: PSModelProxy): Unit = {

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
  def max(from1: PSModelProxy, from2: PSModelProxy, to: PSModelProxy): Unit = {

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
  def min(from1: PSModelProxy, from2: PSModelProxy, to: PSModelProxy): Unit = {

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
  def map(from: PSModelProxy, func: MapFunc, to: PSModelProxy): Unit = {
    from.assertValid()
    to.assertValid()
    from.assertCompatible(to)
    doMap(from, func, to)
  }

  def mapInPlace(proxy: PSModelProxy, func: MapFunc): Unit = {
    proxy.assertValid()
    doMapInPlace(proxy, func)
  }

  /**
   * Process `Zip2MapFunc` for each element of `from1` and `from2` PSVector
   * Notice: it can only be called in th driver.
   */
  def zip2Map(
      from1: PSModelProxy,
      from2: PSModelProxy,
      func: Zip2MapFunc,
      to: PSModelProxy): Unit = {

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
      from1: PSModelProxy,
      from2: PSModelProxy,
      from3: PSModelProxy,
      func: Zip3MapFunc,
      to: PSModelProxy): Unit = {

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
      from: PSModelProxy,
      func: MapWithIndexFunc,
      to: PSModelProxy): Unit = {

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
      from1: PSModelProxy,
      from2: PSModelProxy,
      func: Zip2MapWithIndexFunc,
      to: PSModelProxy): Unit = {

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
      from1: PSModelProxy,
      from2: PSModelProxy,
      from3: PSModelProxy,
      func: Zip3MapWithIndexFunc,
      to: PSModelProxy): Unit = {

    from1.assertValid()
    from2.assertValid()
    from3.assertValid()
    to.assertValid()
    from1.assertCompatible(to)
    from2.assertCompatible(to)
    from3.assertCompatible(to)
    doZip3MapWithIndex(from1, from2, from3, func, to)
  }

  protected def doPull(vector: PSModelProxy): Array[Double]

  protected def doPush(vector: PSModelProxy, array: Array[Double]): Unit

  protected def doFill(
      to: PSModelProxy,
      value: Double): Unit

  protected def doRandomUniform(
      to: PSModelProxy,
      min: Double,
      max: Double): Unit

  protected def doRandomNormal(
      to: PSModelProxy,
      mean: Double,
      stddev: Double): Unit

  protected def doEqual(v1: PSModelProxy, v2: PSModelProxy): Boolean

  protected def doSum(vector: PSModelProxy): Double

  protected def doMax(vector: PSModelProxy): Double

  protected def doMin(vector: PSModelProxy): Double

  protected def doNnz(vector: PSModelProxy): Int

  protected def doAdd(
      from: PSModelProxy,
      value: Double,
      to: PSModelProxy): Unit

  protected def doMul(
      from: PSModelProxy,
      value: Double,
      to: PSModelProxy): Unit

  protected def doDiv(
      from: PSModelProxy,
      value: Double,
      to: PSModelProxy): Unit

  protected def doPow(
      from: PSModelProxy,
      value: Double,
      to: PSModelProxy): Unit

  protected def doSqrt(
      from: PSModelProxy,
      to: PSModelProxy): Unit

  protected def doExp(
      from: PSModelProxy,
      to: PSModelProxy): Unit

  protected def doExpm1(
      from: PSModelProxy,
      to: PSModelProxy): Unit

  protected def doLog(
      from: PSModelProxy,
      to: PSModelProxy): Unit

  protected def doLog1p(
      from: PSModelProxy,
      to: PSModelProxy): Unit

  protected def doLog10(
      from: PSModelProxy,
      to: PSModelProxy): Unit

  protected def doCeil(
      from: PSModelProxy,
      to: PSModelProxy): Unit

  protected def doFloor(
      from: PSModelProxy,
      to: PSModelProxy): Unit

  protected def doRound(
      from: PSModelProxy,
      to: PSModelProxy): Unit

  protected def doAbs(
      from: PSModelProxy,
      to: PSModelProxy): Unit

  protected def doSignum(
      from: PSModelProxy,
      to: PSModelProxy): Unit

  protected def doAdd(
      from1: PSModelProxy,
      from2: PSModelProxy,
      to: PSModelProxy): Unit

  protected def doSub(
      from1: PSModelProxy,
      from2: PSModelProxy,
      to: PSModelProxy): Unit

  protected def doMul(
      from1: PSModelProxy,
      from2: PSModelProxy,
      to: PSModelProxy): Unit

  protected def doDiv(
      from1: PSModelProxy,
      from2: PSModelProxy,
      to: PSModelProxy): Unit

  protected def doMax(
      from1: PSModelProxy,
      from2: PSModelProxy,
      to: PSModelProxy): Unit

  protected def doMin(
      from1: PSModelProxy,
      from2: PSModelProxy,
      to: PSModelProxy): Unit

  protected def doMap(
      from: PSModelProxy,
      func: MapFunc,
      to: PSModelProxy): Unit

  protected def doMapInPlace(proxy: PSModelProxy, func: MapFunc): Unit

  protected def doZip2Map(
      from1: PSModelProxy,
      from2: PSModelProxy,
      func: Zip2MapFunc,
      to: PSModelProxy): Unit

  protected def doZip3Map(
      from1: PSModelProxy,
      from2: PSModelProxy,
      from3: PSModelProxy,
      func: Zip3MapFunc,
      to: PSModelProxy): Unit

  protected def doMapWithIndex(
      from: PSModelProxy,
      func: MapWithIndexFunc,
      to: PSModelProxy): Unit

  protected def doZip2MapWithIndex(
      from1: PSModelProxy,
      from2: PSModelProxy,
      func: Zip2MapWithIndexFunc,
      to: PSModelProxy): Unit

  protected def doZip3MapWithIndex(
      from1: PSModelProxy,
      from2: PSModelProxy,
      from3: PSModelProxy,
      func: Zip3MapWithIndexFunc,
      to: PSModelProxy): Unit


  /* (driver only) BLAS operators */

  /**
   * Corresponding to `BLAS.axpy`
   * Notice: it can only be called in th driver.
   */
  def axpy(a: Double, x: PSModelProxy, y: PSModelProxy): Unit = {

    x.assertValid()
    y.assertValid()
    x.assertCompatible(y)
    doAxpy(a, x, y)
  }

  /**
   * Corresponding to `BLAS.dot`
   * Notice: it can only be called in th driver.
   */
  def dot(x: PSModelProxy, y: PSModelProxy): Double = {

    x.assertValid()
    y.assertValid()
    x.assertCompatible(y)
    doDot(x, y)
  }

  /**
   * Copy `x` to `y`
   * Notice: it can only be called in th driver.
   */
  def copy(x: PSModelProxy, y: PSModelProxy): Unit = {
    x.assertValid()
    y.assertValid()
    x.assertCompatible(y)
    doCopy(x, y)
  }

  /**
   * Corresponding to `BLAS.scal`
   * Notice: it can only be called in th driver.
   */
  def scal(a: Double, x: PSModelProxy): Unit = {
    x.assertValid()
    doScal(a, x)
  }

  /**
   * math.sqrt(x.map(x => x * x).sum)
   * Notice: it can only be called in th driver.
   */
  def nrm2(x: PSModelProxy): Double = {

    x.assertValid()
    doNrm2(x)
  }

  /**
   * Calculate the sum of each element absolute value
   * Notice: it can only be called in th driver.
   */
  def asum(x: PSModelProxy): Double = {

    x.assertValid()
    doAsum(x)
  }

  /**
   * Find the maximum of each element absolute value
   * Notice: it can only be called in th driver.
   */
  def amax(x: PSModelProxy): Double = {

    x.assertValid()
    doAmax(x)
  }

  /**
   * Find the maximum of each element absolute value
   * Notice: it can only be called in th driver.
   */
  def amin(x: PSModelProxy): Double = {

    x.assertValid()
    doAmin(x)
  }

  protected def doAxpy(a: Double, x: PSModelProxy, y: PSModelProxy): Unit

  protected def doDot(x: PSModelProxy, y: PSModelProxy): Double

  protected def doCopy(x: PSModelProxy, y: PSModelProxy): Unit

  protected def doScal(a: Double, x: PSModelProxy): Unit

  protected def doNrm2(x: PSModelProxy): Double

  protected def doAsum(x: PSModelProxy): Double

  protected def doAmax(x: PSModelProxy): Double

  protected def doAmin(x: PSModelProxy): Double


  /* =========================================== */
  /*          executor only methods              */
  /* =========================================== */

  /**
   * Increment `delta` to `vector`.
   * Notice: only be called in executor
   */
  def increment(vector: PSModelProxy, delta: Array[Double]): Unit = {

    vector.assertValid()
    vector.assertCompatible(delta)
    doIncrement(vector, delta)
  }

  /**
   * Find the maximum number of each dimension.
   * Notice: only be called in executor
   */
  def mergeMax(vector: PSModelProxy, other: Array[Double]): Unit = {

    vector.assertValid()
    vector.assertCompatible(other)
    doMergeMax(vector, other)
  }

  /**
   * Find the minimum number of each dimension.
   * Notice: only be called in executor
   */
  def mergeMin(vector: PSModelProxy, other: Array[Double]): Unit = {

    vector.assertValid()
    vector.assertCompatible(other)
    doMergeMin(vector, other)
  }

  protected def doIncrement(vector: PSModelProxy, delta: Array[Double]): Unit

  protected def doMergeMax(vector: PSModelProxy, other: Array[Double]): Unit

  protected def doMergeMin(vector: PSModelProxy, other: Array[Double]): Unit


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
  private var client: PSClient = _

  def apply(): PSClient = {
    if (client == null) {
      classOf[PSClient].synchronized {
        if (client == null) {
          client = new AngelPSClient(PSContext.getOrCreate())
        }
      }
    }
    client
  }
}
