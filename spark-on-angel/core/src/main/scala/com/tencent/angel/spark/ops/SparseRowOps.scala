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
import com.tencent.angel.ml.math.vector.SparseLongKeyDoubleVector
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ArrayAggrResult
import com.tencent.angel.ml.matrix.psf.aggr.{Pull, PullWithCols}
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.get.single.GetRowResult
import com.tencent.angel.ml.matrix.psf.update.{Compress, SparseIncrement, SparsePush}
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc
import com.tencent.angel.psagent.matrix.{MatrixClientFactory, ResponseType, Result}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.linalg.SparseVector
import com.tencent.angel.spark.models.vector.SparsePSVector


/**
 * SparseRowOps contains kinds of operations for sparse rows.
 */
class SparseRowOps {

  /**
   * Pull a specific set of index from SparsePSVector
   * @return SparseVector which contain (index, value) pairs
   */
  def pull(vector: SparsePSVector, indices: Array[Long]): SparseVector = {
    vector.assertValid()
    val rowResult = aggregate(vector.poolId, new PullWithCols(vector.poolId, vector.id, indices))
        .asInstanceOf[ArrayAggrResult]

    new SparseVector(vector.dimension, rowResult.getCols, rowResult.getResult)
  }

  /**
   * Pull a whole SparsePSVector to local.
   */
  def pull(vector: SparsePSVector): SparseVector = {
    vector.assertValid()

    val row = aggregate(vector.poolId, new Pull(vector.poolId, vector.id)).asInstanceOf[GetRowResult]

    row.getRow match {
      case longKeyVector: SparseLongKeyDoubleVector =>
        new SparseVector(longKeyVector.getModelNnz, longKeyVector.getIndexToValueMap)
      case _ =>
        throw new Exception("only support SparseLongKeyDoubleVector")
    }
  }

  /**
   * Push a local sparse vector to SparsePSVector.
   * @param to is the PSVector to push
   * @param local is the local sparse vector, that is (long, double) pairs
   */
  def push(to: SparsePSVector, local: SparseVector): Unit = {
    to.assertValid()
    update(to.poolId, new SparsePush(to.poolId, to.id, local.indices, local.values))
  }

  /**
   * Increment a local sparse vector to SparsePSVector
   * @param vector is the PSVector to increment
   * @param local is the local sparse vector
   */
  def increment(vector: SparsePSVector, local: SparseVector): Unit = {
    vector.assertValid()

    update(vector.poolId, new SparseIncrement(vector.poolId, vector.id, local.indices, local.values))
  }

  private[spark] def increment(poolId: Int, vectorId: Int, local: SparseVector): Unit = {
    update(poolId, new SparseIncrement(poolId, vectorId, local.indices, local.values))
  }

  private[spark] def increment(poolId: Int, vectorId: Int, indices: Array[Long], values: Array[Double]): Unit = {
    update(poolId, new SparseIncrement(poolId, vectorId, indices, values))
  }

  def compress(vector: SparsePSVector): Unit = {
    update(vector.poolId, new Compress(vector.poolId, vector.id))
  }

  /**
   * Apply ps UpdateFunc to a row in matrix
   */
  private def update(matrixId: Int, func: UpdateFunc): Unit = {
    val client = MatrixClientFactory.get(matrixId, PSContext.getTaskId())
    val result = client.update(func).get
    assertSuccess(result)
  }

  /**
   * Use ps Function(PSF) to aggregate a PSVector on PS nodes.
   */
  private[spark] def aggregate(matrixId: Int, func: GetFunc): GetResult = {
    val client = MatrixClientFactory.get(matrixId, PSContext.getTaskId())
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
