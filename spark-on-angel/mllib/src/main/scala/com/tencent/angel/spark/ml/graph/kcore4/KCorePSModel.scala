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
package com.tencent.angel.spark.ml.graph.kcore4

import com.tencent.angel.ml.math2.vector.{LongIntVector, Vector}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import com.tencent.angel.spark.models.impl.PSMatrixImpl
import org.apache.spark.rdd.RDD

private[kcore4] class KCorePSModel(val matrix: PSMatrix) extends Serializable {

  val dim: Long = matrix.columns

  def updateDegreeAndVersion(degree: Vector, version: Vector): Unit =
    matrix.update(Array(0, 1), Array(degree, version))

  def pullVersion(nodes: Array[Long]): LongIntVector =
    matrix.pull(1, nodes).asInstanceOf[LongIntVector]

  def pullDegreeAndVersion(nodes: Array[Long]): (LongIntVector, LongIntVector) = {
    val vectors = matrix.pull(Array(0, 1), nodes).map(f => f.asInstanceOf[LongIntVector])
    (vectors(0), vectors(1))
  }

  def addDegree(degree: Vector): Unit =
    matrix.increment(0, degree)

  def updateVersionAndKcore(version: Vector, kcore: Vector): Unit =
    matrix.update(Array(1, 2), Array(version, kcore))

  def pullDegree(nodes: Array[Long]): LongIntVector =
    matrix.pull(0, nodes).asInstanceOf[LongIntVector]

  def pullKcore(nodes: Array[Long]): LongIntVector =
    matrix.pull(2, nodes).asInstanceOf[LongIntVector]

}


private[kcore4] object KCorePSModel {

  def fromMinMax(minId: Long, maxId: Long, data: RDD[Long], psNumPartition: Int): KCorePSModel = {
    val matrix = new MatrixContext("cores", 3, minId, maxId)
    matrix.setRowType(RowType.T_INT_SPARSE_LONGKEY)
    matrix.setValidIndexNum(0)
    val bits = (numBits(maxId) * 0.7).toInt
    val partitioner = new LoadBalancePartitioner(bits, psNumPartition)
    partitioner.partitionIndex(data, matrix)
    PSAgentContext.get().getMasterClient.createMatrix(matrix, 10000L)
    val matrixId = PSAgentContext.get().getMasterClient.getMatrix("cores").getId
    new KCorePSModel(new PSMatrixImpl(matrixId, matrix.getName, 3, maxId, matrix.getRowType))
  }

  def numBits(maxId: Long): Int = {
    var num = 0
    var value = maxId
    while (value > 0) {
      value >>= 1
      num += 1
    }
    num
  }

}
