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
import com.tencent.angel.ml.math.vector.{DenseDoubleVector, SparseLongKeyDoubleVector}
import com.tencent.angel.ml.matrix.psf.aggr.Pull
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.get.single.GetRowResult
import com.tencent.angel.ml.matrix.psf.update.SparsePush
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc
import com.tencent.angel.psagent.matrix.{MatrixClientFactory, ResponseType, Result}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.vector.{PSVector, SparsePSVector}

class SparseRowOps {

  def push(to: PSVector, pairs: Array[(Long, Double)]): Unit = {
    to.assertValid()
    val (indices, values) = pairs.unzip
    update(to.poolId, new SparsePush(to.poolId, to.id, indices, values))
  }

  def pull(vector: PSVector, indices: Array[Long]): Array[(Long, Double)] = ???

  def pull(vector: SparsePSVector): Array[(Long, Double)] = {
    vector.assertValid()

    val row = aggregate(vector.poolId, new Pull(vector.poolId, vector.id)).asInstanceOf[GetRowResult]
    row.getRow match {
      case vector: SparseLongKeyDoubleVector =>
        val result = new Array[(Long, Double)](vector.size())

        val iter = vector.getIndexToValueMap.long2DoubleEntrySet().fastIterator()
        var index = 0
        while (iter.hasNext) {
          val entry = iter.next()
          result(index) = Tuple2(entry.getLongKey, entry.getDoubleValue)
          index += 1
        }
        result
      case _ =>
        throw new Exception("only support SparseLongKeyDoubleVector")
    }
  }

  def increment(vector: PSVector, pair: Array[(Long, Double)]): Unit = {
    vector.assertValid()
    import com.tencent.angel.ml.matrix.psf.common.{Increment => CommonIncrement}
    val rows = Array.fill(pair.length)(vector.id)
    val (cols, values) = pair.unzip

    update(vector.poolId, new CommonIncrement(vector.poolId, rows, cols, values))
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
