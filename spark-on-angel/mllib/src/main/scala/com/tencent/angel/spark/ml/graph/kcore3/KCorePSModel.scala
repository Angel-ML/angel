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
package com.tencent.angel.spark.ml.graph.kcore3

import com.tencent.angel.ml.math2.vector.{LongIntVector, Vector}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.models.PSMatrix
import com.tencent.angel.spark.models.impl.PSMatrixImpl

private[kcore3] class KCorePSModel(val matrix: PSMatrix) extends Serializable {

  val dim: Long = matrix.columns

  def updateDegree(update: Vector): Unit =
    matrix.update(0, update)

  def updateDegreeAndKcore(degree: Vector, kcore: Vector): Unit =
    matrix.update(Array(0, 1), Array(degree, kcore))

  def pullDegree(nodes: Array[Long]): LongIntVector =
    matrix.pull(0, nodes).asInstanceOf[LongIntVector]

  def pullKcore(nodes: Array[Long]): LongIntVector =
    matrix.pull(1, nodes).asInstanceOf[LongIntVector]

}

private[kcore3] object KCorePSModel {
  def fromMinMax(minId: Long, maxId: Long): KCorePSModel = {
    val matrix = new MatrixContext("cores", 2, minId, maxId)
    matrix.setRowType(RowType.T_INT_SPARSE_LONGKEY)
    PSAgentContext.get().getMasterClient.createMatrix(matrix, 10000L)
    val matrixId = PSAgentContext.get().getMasterClient.getMatrix("cores").getId
    new KCorePSModel(new PSMatrixImpl(matrixId, matrix.getName, 2, maxId, matrix.getRowType))
  }
}
