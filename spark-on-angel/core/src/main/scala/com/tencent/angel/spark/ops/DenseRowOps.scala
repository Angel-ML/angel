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
import com.tencent.angel.ml.math.vector.DenseDoubleVector
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ArrayAggrResult
import com.tencent.angel.ml.matrix.psf.aggr.{Pull, PullWithCols}
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.get.single.GetRowResult
import com.tencent.angel.ml.matrix.psf.update._
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc
import com.tencent.angel.ml.matrix.psf.update.Push
import com.tencent.angel.psagent.matrix.{MatrixClientFactory, ResponseType, Result}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.linalg.{DenseVector, SparseVector}
import com.tencent.angel.spark.models.vector.{DensePSVector, PSVector}

class DenseRowOps {

  // ===========================
  //    Aggregate Operations
  // ===========================
  /**
   * Pull a specific set of index from SparsePSVector
   * @return SparseVector which contain (index, value) pairs
   */
  def pull(vector: DensePSVector, indices: Array[Long]): SparseVector = {
    vector.assertValid()
    val rowResult = aggregate(vector.poolId, new PullWithCols(vector.poolId, vector.id, indices))
      .asInstanceOf[ArrayAggrResult]

    new SparseVector(vector.dimension, rowResult.getCols, rowResult.getResult)
  }


  /**
   * DensePSVector pull function
   */
  def pull(vector: DensePSVector): DenseVector = {
    vector.assertValid()
    val row = aggregate(vector.poolId, new Pull(vector.poolId, vector.id))
      .asInstanceOf[GetRowResult]
    new DenseVector(row.getRow.asInstanceOf[DenseDoubleVector].getValues)
  }


  // ===========================
  //    Update Operations
  // ===========================
  /**
   * Generate a random PSVector, the random distribution is uniform.
   *
   * @param min the minimum of uniform distribution
   * @param max the maximum of uniform distribution
   */
  def randomUniform(to: DensePSVector, min: Double, max: Double): Unit = {
    to.assertValid()
    update(to.poolId, new RandomUniform(to.poolId, to.id, min, max))
  }

  /**
   * Generate a random PSVector, the random distribution is normal distribution.
   *
   * @param mean the `mean` parameter of uniform distribution
   * @param stddev the `stddev` parameter of uniform distribution
   */
  def randomNormal(to: DensePSVector, mean: Double, stddev: Double): Unit = {
    to.assertValid()
    update(to.poolId, new RandomNormal(to.poolId, to.id, mean, stddev))
  }

  /**
   * Put `value` to PSVectorKey
   */
  def push(to: DensePSVector, array: Array[Double]): Unit = {
    to.assertValid()
    to.assertCompatible(array)
    update(to.poolId, new Push(to.poolId, to.id, array))
  }

  def push(to: DensePSVector, local: DenseVector): Unit = {
    push(to, local.values)
  }

  def fill(to: DensePSVector, values: Array[Double]) = push(to, values)

  /**
   * Increment `delta` to `vector`.
   */
  def increment(vector: DensePSVector, local: DenseVector): Unit = {
    increment(vector, local.values)
  }

  def increment(vector: DensePSVector, delta: Array[Double]): Unit = {
    vector.assertValid()
    vector.assertCompatible(delta)
    update(vector.poolId, new Increment(vector.poolId, vector.id, delta))
  }

  private[spark] def increment(poolId: Int, vectorId: Int, delta: Array[Double]): Unit = {
    update(poolId, new Increment(poolId, vectorId, delta))
  }

  /**
   * Find the maximum number of each dimension.
   * Notice: only be called in executor
   */
  def mergeMax(vector: DensePSVector, other: DenseVector): Unit = {
    mergeMax(vector, other.values)
  }

  def mergeMax(vector: DensePSVector, other: Array[Double]): Unit = {
    vector.assertValid()
    vector.assertCompatible(other)
    update(vector.poolId, new MaxA(vector.poolId, vector.id, other))
  }

  private[spark] def mergeMax(poolId: Int, vectorId: Int, other: Array[Double]): Unit = {
    update(poolId, new MaxA(poolId, vectorId, other))
  }

  /**
   * Find the minimum number of each dimension.
   * Notice: only be called in executor
   */
  def mergeMin(vector: DensePSVector, other: DenseVector): Unit = {
    mergeMin(vector, other.values)
  }

  def mergeMin(vector: DensePSVector, other: Array[Double]): Unit = {
    vector.assertValid()
    vector.assertCompatible(other)
    update(vector.poolId, new MinA(vector.poolId, vector.id, other))
  }

  private[spark] def mergeMin(poolId: Int, vectorId: Int, other: Array[Double]): Unit = {
    update(poolId, new MinA(poolId, vectorId, other))
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

  private def update(matrixId: Int, func: UpdateFunc): Unit = {
    val client = MatrixClientFactory.get(matrixId, PSContext.getTaskId())
    val result = client.update(func).get
    assertSuccess(result)
  }

  private def assertSuccess(result: Result): Unit = {
    if (result.getResponseType == ResponseType.FAILED) {
      throw new AngelException("PS computation failed!")
    }
  }
}
