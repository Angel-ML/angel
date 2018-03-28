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
 */

package com.tencent.angel.spark.ops

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.matrix.psf.aggr._
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarAggrResult
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.update._
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc
import com.tencent.angel.ml.matrix.psf.update.enhance.map._
import com.tencent.angel.ml.matrix.psf.update.enhance.zip2.{Zip2Map, Zip2MapFunc, Zip2MapWithIndex, Zip2MapWithIndexFunc}
import com.tencent.angel.ml.matrix.psf.update.enhance.zip3.{Zip3Map, Zip3MapFunc, Zip3MapWithIndex, Zip3MapWithIndexFunc}
import com.tencent.angel.psagent.matrix.{MatrixClientFactory, ResponseType, Result}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.vector.PSVector

class VectorOps {

  /**
   * Process `MapFunc` for each element of `from` PSVector
   */
  def map(from: PSVector, func: MapFunc, to: PSVector): Unit = {
    from.assertValid()
    to.assertValid()
    to.assertCompatible(to)
    update(from.poolId, new Map(from.poolId, from.id, to.id, func))
  }

  def mapInPlace(proxy: PSVector, func: MapFunc): Unit = {
    proxy.assertValid()
    update(proxy.poolId, new MapInPlace(proxy.poolId, proxy.id, func))
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

    from1.assertCompatible(from2, to)
    update(from1.poolId, new Zip2Map(from1.poolId, from1.id, from2.id, to.id, func))
  }


  /**
   * Process `Zip3MapFunc` for each element of `from1`,
   * `from2` and `from3` PSVector
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
    from1.assertCompatible(from2, from3, to)

    update(from1.poolId,
      new Zip3Map(from1.poolId, from1.id, from2.id, from3.id, to.id, func))
  }

  /**
   * Process `MapWithIndexFunc` for each element of `from` PSVector
   */
  def mapWithIndex(
      from: PSVector,
      func: MapWithIndexFunc,
      to: PSVector): Unit = {

    from.assertValid()
    to.assertValid()
    from.assertCompatible(to)
    update(from.poolId, new MapWithIndex(from.poolId, from.id, to.id, func))
  }

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
    from1.assertCompatible(from2, to)

    update(from1.poolId,
      new Zip2MapWithIndex(from1.poolId, from1.id, from2.id, to.id, func))
  }

  /**
   * Process `Zip3MapWithIndexFunc` for each element of `from1`,
   * `from2` and `from3` PSVector
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
    from1.assertCompatible(from2, from3, to)

    update(from1.poolId,
      new Zip3MapWithIndex(from1.poolId, from1.id, from2.id, from3.id, to.id, func))
  }


  // ===========================
  //    Aggregate Operations
  // ===========================

  /**
   * Judge if v1 is equal to v2 by element-wise.
   */
  def equal(v1: PSVector, v2: PSVector): Boolean = {
    v1.assertCompatible(v2)
    val res = aggregate(v1.poolId, new Equal(v1.poolId, v1.id, v2.id)).asInstanceOf[ScalarAggrResult].getResult
    res == 1.0
  }

  /**
   * Sum all the dimension of `vector`
   */
  def sum(vector: PSVector): Double = {
    vector.assertValid()
    aggregate(vector.poolId, new Sum(vector.poolId, vector.id)).asInstanceOf[ScalarAggrResult].getResult
  }

  /**
   * Find the maximum element of `vector`
   */
  def max(vector: PSVector): Double = {
    vector.assertValid()
    aggregate(vector.poolId, new Max(vector.poolId, vector.id)).asInstanceOf[ScalarAggrResult].getResult
  }

  /**
   * Find the minimum element of `vector`
   */
  def min(vector: PSVector): Double = {
    vector.assertValid()
    aggregate(vector.poolId, new Min(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  /**
   * Count the number of non-zero element in `vector`
   */
  def nnz(vector: PSVector): Int = {
    vector.assertValid()
    aggregate(vector.poolId, new Nnz(vector.poolId, vector.id))
      .asInstanceOf[ScalarAggrResult].getResult.toInt
  }


  // ===========================
  //    Update Operations
  // ===========================
  /**
   * Fill PSVectorKey with `value`
   * Notice: it can only be called in th driver.
   */
  def fill(to: PSVector, value: Double): Unit = {
    to.assertValid()
    update(to.poolId, new Fill(to.poolId, to.id, value))
  }

  /**
   * Add `from1` PSVector and `from2` PSVector to `to` PSVector
   */
  def add(from1: PSVector, from2: PSVector, to: PSVector): Unit = {
    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    from1.assertCompatible(from2, to)
    update(from1.poolId, new Add(from1.poolId, from1.id, from2.id, to.id))
  }


  /**
   * Subtract `from2` PSVector from `from1` PSVector and save result to `to` PSVector
   */
  def sub(from1: PSVector, from2: PSVector, to: PSVector): Unit = {
    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    from1.assertCompatible(from2, to)
    update(from1.poolId, new Sub(from1.poolId, from1.id, from2.id, to.id))
  }

  /**
   * Multiply `from1` PSVector and `from2` PSVector and save result to `to` PSVector
   */
  def mul(from1: PSVector, from2: PSVector, to: PSVector): Unit = {
    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    from1.assertCompatible(from2, to)
    update(from1.poolId, new Mul(from1.poolId, from1.id, from2.id, to.id))
  }


  /**
   * Divide `from1` PSVector to `from2` PSVector and save result to `to` PSVector
   */
  def div(from1: PSVector, from2: PSVector, to: PSVector): Unit = {
    from1.assertValid()
    from2.assertValid()
    to.assertValid()
    from1.assertCompatible(from2, to)
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
    from1.assertCompatible(from2, to)
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
    from1.assertCompatible(from2, to)
    update(from1.poolId, new MinV(from1.poolId, from1.id, from2.id, to.id))
  }


  /**
   * Corresponding to `BLAS.axpy`
   */
  def axpy(a: Double, x: PSVector, y: PSVector): Unit = {
    x.assertValid()
    y.assertValid()
    x.assertCompatible(y)
    update(x.poolId, new Axpy(x.poolId, x.id, y.id, a))
  }

  /**
   * Corresponding to `BLAS.dot`
   */
  def dot(x: PSVector, y: PSVector): Double = {
    x.assertValid()
    y.assertValid()
    x.assertCompatible(y)
    aggregate(x.poolId, new Dot(x.poolId, x.id, y.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  /**
   * Copy `x` to `y`
   */
  def copy(x: PSVector, y: PSVector): Unit = {
    x.assertValid()
    y.assertValid()
    x.assertCompatible(y)
    update(x.poolId, new Copy(x.poolId, x.id, y.id))
  }

  /**
   * math.sqrt(x.map(x => x * x).sum)
   */
  def nrm2(x: PSVector): Double = {
    x.assertValid()
    aggregate(x.poolId, new Nrm2(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  /**
   * Calculate the sum of each element absolute value
   */
  def asum(x: PSVector): Double = {
    x.assertValid()
    aggregate(x.poolId, new Asum(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  /**
   * Find the maximum of each element absolute value
   */
  def amax(x: PSVector): Double = {
    x.assertValid()
    aggregate(x.poolId, new Amax(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  /**
   * Find the maximum of each element absolute value
   */
  def amin(x: PSVector): Double = {
    x.assertValid()
    aggregate(x.poolId, new Amin(x.poolId, x.id))
      .asInstanceOf[ScalarAggrResult].getResult
  }

  private def update(modelId: Int, func: UpdateFunc): Unit = {
    val client = MatrixClientFactory.get(modelId, PSContext.getTaskId())
    val result = client.update(func).get
    assertSuccess(result)
  }

  /**
   * Use ps Function(PSF) to aggregate a PSVector on PS nodes.
   */
  private[spark] def aggregate(modelId: Int, func: GetFunc): GetResult = {
    val client = MatrixClientFactory.get(modelId, PSContext.getTaskId())
    val result = client.get(func)
    assertSuccess(result)
    result
  }

  private def assertSuccess(result: Result): Unit = {
    if (result.getResponseType == ResponseType.FAILED) {
      throw new AngelException("PS computation failed!")
    }
  }
}
