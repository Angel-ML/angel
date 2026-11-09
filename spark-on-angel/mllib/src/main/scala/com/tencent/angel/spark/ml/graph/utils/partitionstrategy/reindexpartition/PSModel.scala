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
package com.tencent.angel.spark.ml.graph.utils.partitionstrategy.reindexpartition

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{LongIntVector, LongLongVector, Vector}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.util.VectorUtils
import org.apache.spark.rdd.RDD

class PSModel(val newIndex: PSVector) extends Serializable {
  val dim: Long = newIndex.dimension

//  def initMsgs(msgs: LongLongVector): Unit = {
//    newIndex.update(msgs)
//  }

  def initMsgs(pairs: Seq[(Long, Long)]): Int = {
    val msgs = VFactory.sparseLongKeyLongVector(pairs.length)
    pairs.foreach { case (key, newKey) =>
      msgs.set(key, newKey)
    }
    newIndex.update(msgs)
    pairs.length
  }

  def readMsgs(nodes: Array[Long]): LongLongVector =
    newIndex.pull(nodes).asInstanceOf[LongLongVector]

  def readAllMsgs(): LongLongVector =
    newIndex.pull().asInstanceOf[LongLongVector]

}

object PSModel {

  def fromMinMax(minId: Long, maxId: Long, data: RDD[Long], psNumPartition: Int, useBalancePartition: Boolean): PSModel = {
    val matrix = new MatrixContext("newIndex", 1, minId, maxId)
    matrix.setValidIndexNum(-1)
    matrix.setRowType(RowType.T_LONG_SPARSE_LONGKEY)
    // use balance partition
    if (useBalancePartition)
      LoadBalancePartitioner.partition(data, maxId, psNumPartition, matrix)

    PSAgentContext.get().getMasterClient.createMatrix(matrix, 10000L)
    val matrixId = PSAgentContext.get().getMasterClient.getMatrix("newIndex").getId
    new PSModel(new PSVectorImpl(matrixId, 0, maxId, matrix.getRowType))
  }

}
