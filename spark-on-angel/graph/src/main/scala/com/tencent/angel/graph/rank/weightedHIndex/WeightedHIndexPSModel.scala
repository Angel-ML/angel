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
package com.tencent.angel.graph.rank.weightedHIndex

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.math2.vector.{LongFloatVector, Vector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.models.{PSMatrix, PSVector}
import com.tencent.angel.spark.util.VectorUtils


class WeightedHIndexPSModel(modelContext: ModelContext) extends Serializable {

  var nodesS: PSVector = _
  var psMatrix: PSMatrix = _
  var dim: Long = _

  def init(): Unit = {
    val mc = ModelContextUtils.createMatrixContext(modelContext, RowType.T_FLOAT_SPARSE_LONGKEY)
    psMatrix = PSMatrix.matrix(mc)
    val matrixId = psMatrix.id
    nodesS = new PSVectorImpl(matrixId, 0, modelContext.getMaxNodeId, mc.getRowType)
    dim = nodesS.dimension
  }

  def updateNodesS(msgs: Vector): Unit =
    nodesS.update(msgs)

  def readNodesS(nodes: Array[Long]): LongFloatVector =
    nodesS.pull(nodes).asInstanceOf[LongFloatVector]

  def readAllNodesS(): LongFloatVector =
    nodesS.pull().asInstanceOf[LongFloatVector]

  def numMsgs(): Long = VectorUtils.nnz(nodesS)

  def checkpoint(): Unit = psMatrix.checkpoint()

}
