/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.spark.util

import java.util.concurrent.Future

import org.apache.spark.SparkException
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.matrix.psf.aggr._
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarAggrResult
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.update.base.{UpdateFunc, VoidResult}
import com.tencent.angel.ml.matrix.psf.update.enhance.map._
import com.tencent.angel.ml.matrix.psf.update.enhance.map.func.{Abs, DivS, MapFunc, MulS, Sqrt}
import com.tencent.angel.ml.matrix.psf.update.enhance.zip2._
import com.tencent.angel.ml.matrix.psf.update.enhance.zip2.func.{Zip2MapFunc, Zip2MapWithIndexFunc}
import com.tencent.angel.ml.matrix.psf.update.{Compress, RandomNormal, RandomUniform}
import com.tencent.angel.psagent.matrix.{MatrixClientFactory, ResponseType, Result}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.PSVector

object VectorUtils {

  /**
   * Process `MapFunc` for each element of `from` PSVector
   */
  def map(from: PSVector, func: MapFunc, to: PSVector): Unit = {
    from.assertValid()
    to.assertValid()
    assertCompatible(from, to)
    update(from.poolId, new Map(from.poolId, from.id, to.id, func))
  }

  private[spark] def update(modelId: Int, func: UpdateFunc): Unit = {
    val client = MatrixClientFactory.get(modelId, PSContext.getTaskId)
    val result = client.asyncUpdate(func).get()
    assertSuccess(result)
  }

  private def assertSuccess(result: Result): Unit = {
    if (result.getResponseType == ResponseType.FAILED) {
      throw new AngelException("PS computation failed!")
    }
  }

  def mapInPlace(proxy: PSVector, func: MapFunc): Unit = {
    proxy.assertValid()
    update(proxy.poolId, new MapInPlace(proxy.poolId, proxy.id, func))
  }

  def iabs(proxy: PSVector): Unit = {
    proxy.assertValid()
    update(proxy.poolId, new MapInPlace(proxy.poolId, proxy.id, new Abs(true)))
  }

  def imul(proxy: PSVector, x: Double): Unit = {
    proxy.assertValid()
    update(proxy.poolId, new MapInPlace(proxy.poolId, proxy.id, new MulS(x, true)))
  }

  def idiv(proxy: PSVector, x: Double): Unit = {
    proxy.assertValid()
    update(proxy.poolId, new MapInPlace(proxy.poolId, proxy.id, new DivS(x, true)))
  }

  def isqrt(proxy: PSVector): Unit = {
    proxy.assertValid()
    update(proxy.poolId, new MapInPlace(proxy.poolId, proxy.id, new Sqrt(true)))
  }

  /**
   * Process `Zip2MapFunc` for each element of `from1` and `from2` PSVector
   */
  def zip2Map(
      from1: PSVector,
      from2: PSVector,
      func: Zip2MapFunc,
      to: PSVector): Unit = {

    from1.assertValid()
    from2.assertValid()
    to.assertValid()

    assertCompatible(from1, from2, to)
    update(from1.poolId, new Zip2Map(from1.poolId, from1.id, from2.id, to.id, func))
  }

  /**
   * Process `MapWithIndexFunc` for each element of `from` PSVector
   */
  def mapWithIndex(
      from: PSVector,
      func: MapFunc,
      to: PSVector): Unit = {

    from.assertValid()
    to.assertValid()
    assertCompatible(from, to)
    update(from.poolId, new MapWithIndex(from.poolId, from.id, to.id, func))
  }


  // ===========================
  //    Aggregate Operations
  // ===========================

  /**
   * Process `zip2MapWithIndex` for each element of `from1` and `from2` PSVector
   */
  def zip2MapWithIndex(
      from1: PSVector,
      from2: PSVector,
      func: Zip2MapWithIndexFunc,
      to: PSVector): Unit = {

    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    assertCompatible(from1, from2, to)

    update(from1.poolId,
      new Zip2MapWithIndex(from1.poolId, from1.id, from2.id, to.id, func))
  }

  /**
   * Sum all the dimension of `vector`
   */
  def sum(vector: PSVector): Double = {
    vector.assertValid()
    psfGet(vector.poolId, new Sum(vector.poolId, vector.id)).asInstanceOf[ScalarAggrResult].getResult
  }

  /**
   * Find the maximum element of `vector`
   */
  def max(vector: PSVector): Double = {
    vector.assertValid()
    psfGet(vector.poolId, new Max(vector.poolId, vector.id)).asInstanceOf[ScalarAggrResult].getResult
  }


  // ===========================
  //    Update Operations
  // ===========================

  /**
   * Find the minimum element of `vector`
   */
  def min(vector: PSVector): Double = {
    vector.assertValid()
    psfGet(vector.poolId, new Min(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  /**
   * Count the number of non-zero element in `vector`
   */
  def nnz(vector: PSVector): Long = {
    vector.assertValid()
    psfGet(vector.poolId, new Nnz(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult.toLong
  }

  /**
    * Count the number of elements in `vector`
    */
  def size(vector: PSVector): Long = {
    vector.assertValid()
    psfGet(vector.poolId, new Size(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult.toLong
  }

  /**
   * Use ps Function(PSF) to aggregate a PSVector on PS nodes.
   */
  private[spark] def psfGet(modelId: Int, func: GetFunc): GetResult = {
    val client = MatrixClientFactory.get(modelId, PSContext.getTaskId)
    val result = client.get(func)
    assertSuccess(result)
    result
  }

  /**
   * Use ps Function(PSF) to aggregate a PSVector on PS nodes.
   */
  private[spark] def psfUpdate(modelId: Int, func: UpdateFunc): Future[VoidResult] = {
    val client = MatrixClientFactory.get(modelId, PSContext.getTaskId)
    client.asyncUpdate(func)
  }


  /**
   * Add `from1` PSVector and `from2` PSVector to `to` PSVector
   */
  def add(from1: PSVector, from2: PSVector, to: PSVector): Unit = {
    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    assertCompatible(from1, from2, to)
    update(from1.poolId, new Add(from1.poolId, from1.id, from2.id, to.id))
  }

  /**
   * Subtract `from2` PSVector from `from1` PSVector and save result to `to` PSVector
   */
  def sub(from1: PSVector, from2: PSVector, to: PSVector): Unit = {
    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    assertCompatible(from1, from2, to)
    update(from1.poolId, new Sub(from1.poolId, from1.id, from2.id, to.id))
  }

  /**
   * Multiply `from1` PSVector and `from2` PSVector and save result to `to` PSVector
   */
  def mul(from1: PSVector, from2: PSVector, to: PSVector): Unit = {
    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    assertCompatible(from1, from2, to)
    update(from1.poolId, new Mul(from1.poolId, from1.id, from2.id, to.id))
  }

  /**
   * Divide `from1` PSVector to `from2` PSVector and save result to `to` PSVector
   */
  def div(from1: PSVector, from2: PSVector, to: PSVector): Unit = {
    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    assertCompatible(from1, from2, to)
    update(from1.poolId, new Div(from1.poolId, from1.id, from2.id, to.id))
  }

  /**
   * Find the maximum element of `from1` and `from2` PSVector,
   * and save result to `to` PSVector
   */
  def max(from1: PSVector, from2: PSVector, to: PSVector): Unit = {
    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    assertCompatible(from1, from2, to)
    update(from1.poolId, new MaxV(from1.poolId, from1.id, from2.id, to.id))
  }

  /**
   * Find the minimum element of `from1` and `from2` PSVector,
   * and save result to `to` PSVector
   */
  def min(from1: PSVector, from2: PSVector, to: PSVector): Unit = {
    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    assertCompatible(from1, from2, to)
    update(from1.poolId, new MinV(from1.poolId, from1.id, from2.id, to.id))
  }

  /**
   * Corresponding to `BLAS.axpy`
   */
  def axpy(a: Double, x: PSVector, y: PSVector): Unit = {
    x.assertValid()
    y.assertValid()
    assertCompatible(x, y)
    update(x.poolId, new Axpy(x.poolId, x.id, y.id, a))
  }

  /**
   * Corresponding to `BLAS.dot`
   */
  def dot(x: PSVector, y: PSVector): Double = {
    x.assertValid()
    y.assertValid()
    assertCompatible(x, y)
    psfGet(x.poolId, new Dot(x.poolId, x.id, y.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  /**
   * Copy `x` to `y`
   */
  def copy(x: PSVector, y: PSVector): Unit = {
    x.assertValid()
    y.assertValid()
    assertCompatible(x, y)
    update(x.poolId, new Copy(x.poolId, x.id, y.id))
  }

  /**
   * math.sqrt(x.map(x => x * x).sum)
   */
  def nrm2(x: PSVector): Double = {
    x.assertValid()
    psfGet(x.poolId, new Nrm2(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }


  def randomUniform(vec: PSVector, min: Double, max: Double): PSVector = {
    vec.assertValid()
    psfUpdate(vec.poolId, new RandomUniform(vec.poolId, vec.id, min, max)).get()
    vec
  }

  def randomNormal(vec: PSVector, mean: Double, stddev: Double): PSVector = {
    vec.assertValid()
    psfUpdate(vec.poolId, new RandomNormal(vec.poolId, vec.id, mean, stddev)).get()
    vec
  }

  def compress(vec: PSVector): PSVector = {
    vec.assertValid()
    assert(vec.rowType.isSparse,
      s"only sparse row supports compress operation, while rowType ${vec.rowType} given")
    psfUpdate(vec.poolId, new Compress(vec.poolId, vec.id)).get()
    vec
  }

  private[spark] def assertCompatible(others: PSVector*): Unit = {
    val poolId = others.head.poolId
    for (other <- others.tail) {
      if (poolId != other.poolId) {
        throw new SparkException("Operators can only " +
          "be performed on vectors of the same pool!")
      }
    }
  }
}
