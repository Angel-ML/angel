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
package com.tencent.angel.spark.ml.psf.closeness

import com.tencent.angel.ml.matrix.psf.update.enhance.{MMUpdateFunc, MMUpdateParam}
import com.tencent.angel.ps.storage.vector.{ServerLongFloatRow, ServerRow}

/*
  psFunc for saving closeness centrality to the first row of the PSMatrix. Used for Closeness algorithm
   */
class CalcCloseness(param: MMUpdateParam) extends MMUpdateFunc(param) {

  def this(matrixId: Int, rowId: Int, numNodes: Long) =
    this(new MMUpdateParam(matrixId, Array[Int](rowId), Array[Double](numNodes)))

  def this(matrixId: Int, startId: Int, length: Int, numNodes: Long) =
    this(new MMUpdateParam(matrixId, startId, length, Array[Double](numNodes)))

  def this() = this(null)

  override protected def update(rows: Array[ServerRow], scalars: Array[Double]): Unit = {

    val numNodes = scalars(0)
    val cols = (rows(0).getStartCol to rows(0).getEndCol).toArray
    val closeness = cols.map {
      col =>
        val validDists = rows.map(_.asInstanceOf[ServerLongFloatRow].get(col))
          .filter(_ != Float.PositiveInfinity)
        if (validDists.length == 0) 0.0f
        else {
          val meanDist = validDists.sum / validDists.length
          ((numNodes - 1) / (numNodes * meanDist)).toFloat
        }
    }

    rows(0).asInstanceOf[ServerLongFloatRow].set(cols, closeness)
  }
}
