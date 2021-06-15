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
package com.tencent.angel.graph.rank.hindex

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.math2.vector.{LongIntVector, Vector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.models.{PSMatrix, PSVector}
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.util.VectorUtils


class HIndexPSModel(modelContext: ModelContext) extends Serializable {

  var inMsgs: PSVector = _
  var psMatrix: PSMatrix = _
  var dim: Long = _

  def init(): Unit = {
    val mc = ModelContextUtils.createMatrixContext(modelContext, RowType.T_INT_SPARSE_LONGKEY)
    psMatrix = PSMatrix.matrix(mc)
    val matrixId = psMatrix.id
    inMsgs = new PSVectorImpl(matrixId, 0, modelContext.getMaxNodeId, mc.getRowType)
    dim = inMsgs.dimension
  }

  def initMsgs(msgs: Vector): Unit =
    inMsgs.update(msgs)

  def readMsgs(nodes: Array[Long]): LongIntVector =
    inMsgs.pull(nodes).asInstanceOf[LongIntVector]

  def numMsgs(): Long =
    VectorUtils.nnz(inMsgs)

  def checkpoint(): Unit = { psMatrix.checkpoint() }
}
