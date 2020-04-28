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
package com.tencent.angel.spark.ml.graph.connectedcomponent.wcc

import com.tencent.angel.ml.math2.vector.{LongLongVector, Vector}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.util.VectorUtils
import org.apache.spark.rdd.RDD

class WCCPSModel(var inMsgs: PSVector,
									var outMsgs: PSVector) extends Serializable {
	val dim: Long = inMsgs.dimension

	def initMsgs(msgs: Vector): Unit = {
		inMsgs.update(msgs)
	}

	def readMsgs(nodes: Array[Long]): LongLongVector = {
		inMsgs.pull(nodes).asInstanceOf[LongLongVector]
	}
	
	def readAllMsgs(): LongLongVector = {
		inMsgs.pull().asInstanceOf[LongLongVector]
	}

	def writeMsgs(msgs: Vector): Unit = {
		inMsgs.update(msgs)
	}

	def numMsgs(): Long = {
		VectorUtils.nnz(inMsgs)
	}

}

object WCCPSModel {
	def fromMinMax(minId: Long, maxId: Long, data: RDD[Long], psNumPartition: Int,
									useBalancePartition: Boolean, balancePartitionPercent: Float): WCCPSModel = {
		val matrix = new MatrixContext("labels", 2, minId, maxId)
		matrix.setValidIndexNum(-1)
		matrix.setRowType(RowType.T_LONG_SPARSE_LONGKEY)
		
		if (useBalancePartition) {
			LoadBalancePartitioner.partition(data, maxId, psNumPartition, matrix, balancePartitionPercent)
		}

		PSAgentContext.get().getMasterClient.createMatrix(matrix, 10000L)
		val matrixId = PSAgentContext.get().getMasterClient.getMatrix("labels").getId
		new WCCPSModel(new PSVectorImpl(matrixId, 0, maxId, matrix.getRowType),
			new PSVectorImpl(matrixId, 1, maxId, matrix.getRowType))
	}
}
