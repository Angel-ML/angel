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
package com.tencent.angel.graph.connectedcomponent.wcc

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.math2.vector.{LongLongVector, Vector}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.{PSMatrix, PSVector}
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.util.VectorUtils
import org.apache.spark.rdd.RDD

class WCCPSModel(var psVector: PSVector) extends Serializable {
  val dim: Long = psVector.dimension

  /**
    * init ccid for each node
    *
    * @param msgs < node,ccId > key-value vector
    */
  def initMsgs(msgs: Vector): Unit = {
    psVector.update(msgs)
  }

  /**
    * read nodes ccids
    *
    * @param nodes < node,ccId > key-value vector
    * @return
    */
  def readMsgs(nodes: Array[Long]): LongLongVector = {
    psVector.pull(nodes).asInstanceOf[LongLongVector]
  }

  /**
    * when only a little nodes active(changed ccid), read all messages
    *
    * @return < node,ccId > key-value vector
    */
  def readAllMsgs(): LongLongVector = {
    psVector.pull().asInstanceOf[LongLongVector]
  }

  /**
    * write ccids to nodes
    *
    * @param msgs < node,ccId > key-value vector
    */
  def writeMsgs(msgs: Vector): Unit = {
    psVector.update(msgs)
  }

  /**
    * active messages num
    *
    * @return
    */
  def numMsgs(): Long = {
    VectorUtils.nnz(psVector)
  }
}

object WCCPSModel {
  def apply(modelContext: ModelContext, edges: RDD[(Long, Long)], useBalancePartition: Boolean,
            balancePartitionPercent: Float): WCCPSModel = {
    val matrix = ModelContextUtils.createMatrixContext(modelContext, RowType.T_LONG_SPARSE_LONGKEY)
  
    // TODO: remove later
    if (!modelContext.isUseHashPartition && useBalancePartition) {
      val nodes = edges.flatMap(e => Iterator(e._1, e._2))
      LoadBalancePartitioner.partition(
        nodes, modelContext.getMaxNodeId, modelContext.getPartitionNum, matrix, balancePartitionPercent)
    }
  
    val psMatrix = PSMatrix.matrix(matrix)
    new WCCPSModel(new PSVectorImpl(psMatrix.id, 0, modelContext.getMaxNodeId, matrix.getRowType))
  }
}
