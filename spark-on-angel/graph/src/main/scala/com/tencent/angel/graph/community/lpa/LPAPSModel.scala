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
package com.tencent.angel.graph.community.lpa

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.math2.vector.{LongIntVector, LongLongVector, Vector}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.{PSMatrix, PSVector}
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.util.VectorUtils
import org.apache.spark.rdd.RDD

class LPAPSModel(var inMsgs: PSVector,
                 var outMsgs: PSVector) extends Serializable {
  val dim: Long = inMsgs.dimension
  
  
  def initMsgs(msgs: Vector): Unit =
    inMsgs.update(msgs)
  
  def readMsgs(nodes: Array[Long]): LongLongVector =
    inMsgs.pull(nodes).asInstanceOf[LongLongVector]
  
  def readAllMsgs(): LongIntVector =
    inMsgs.pull().asInstanceOf[LongIntVector]
  
  def writeMsgs(msgs: Vector): Unit =
    outMsgs.update(msgs)
  
  def numMsgs(): Long =
    VectorUtils.nnz(inMsgs)
  
  def resetMsgs(): Unit = {
    val temp = inMsgs
    inMsgs = outMsgs
    outMsgs = temp
    outMsgs.reset
  }
  
}

object LPAPSModel {
  
  def apply(modelContext: ModelContext, edges: RDD[(Long, Long)], useBalancePartition: Boolean,
            balancePartitionPercent: Float): LPAPSModel = {
    val matrix = ModelContextUtils.createMatrixContext(modelContext, RowType.T_LONG_SPARSE_LONGKEY, 2)
    
    // TODO: remove later
    if (!modelContext.isUseHashPartition && useBalancePartition) {
      val nodes = edges.flatMap(e => Iterator(e._1, e._2))
      LoadBalancePartitioner.partition(
        nodes, modelContext.getMaxNodeId, modelContext.getPartitionNum, matrix, balancePartitionPercent)
    }
    
    val psMatrix = PSMatrix.matrix(matrix)
    new LPAPSModel(new PSVectorImpl(psMatrix.id, 0, modelContext.getMaxNodeId, matrix.getRowType),
      new PSVectorImpl(psMatrix.id, 1, modelContext.getMaxNodeId, matrix.getRowType))
  }
  
}
