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

package com.tencent.angel.graph

import com.tencent.angel.graph.data.GraphNode
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.partitioner.{ColumnRangePartitioner, HashPartitioner}
import com.tencent.angel.ps.storage.vector.element.IElement
import com.tencent.angel.graph.data.VertexId
import com.tencent.angel.graph.ops.{GetOps, InitOps, SampleOps, SummaryOps}
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.rdd.RDD

/**
  * A base graph model
  * @param param base graph param
  */
class GraphModel(val param: Param) extends Serializable {
  val graphName: String = param.matrixName
  var graphMatrix: PSMatrix = _
  val initOps = new InitOps(this)
  val getOps = new GetOps(this)
  val sampleOps = new SampleOps(this)
  val summaryOps = new SummaryOps(this)

  def createGraph(rowType: RowType = RowType.T_ANY_LONGKEY_SPARSE,
                  valueClass: Class[_ <: IElement] = classOf[GraphNode],
                  useHashPartitioner: Boolean = false,
                  nodes: RDD[VertexId] = null): GraphModel = {
    val mc: MatrixContext = new MatrixContext()
    mc.setName(graphName)
    mc.setRowType(rowType)
    mc.setRowNum(1)
    mc.setValueType(valueClass)
    if (!useHashPartitioner) {
      mc.setIndexStart(param.minIndex)
      mc.setIndexEnd(param.maxIndex)
      mc.setColNum(param.maxIndex - param.minIndex)
      mc.setMaxColNumInBlock((param.maxIndex - param.minIndex) / param.psPartNum)
      mc.setPartitionerClass(classOf[ColumnRangePartitioner])
      if (param.useBalancePartition && nodes != null) {
        LoadBalancePartitioner.partition(nodes, param.psPartNum, mc)
      }
    } else {
      mc.setPartitionNum(param.psPartNum)
      mc.setColNum(param.numNodes)
      mc.setPartitionerClass(classOf[HashPartitioner])
    }
    graphMatrix = PSMatrix.matrix(mc)
    this
  }

  def checkpoint(): Unit = {
    println(s"graph checkpoint now matrixId=${graphMatrix.id}")
    graphMatrix.checkpoint()
  }
}

object GraphModel {

  def apply(maxIndex: Long, batchSize: Int, pullBatch: Int,
            psPartNum: Int,  numNodes: Long, minIndex: Long,
            matrixName: String = "BaseGraph", useBalancePartition: Boolean = false): GraphModel = {
    val param = new Param(maxIndex, batchSize, pullBatch, psPartNum, numNodes,
      minIndex, matrixName, useBalancePartition = useBalancePartition)
    new GraphModel(param)
  }

  implicit def toInitOps(model: GraphModel): InitOps = model.initOps
  implicit def toGetOps(model: GraphModel): GetOps = model.getOps
  implicit def toSampleOps(model: GraphModel): SampleOps = model.sampleOps
  implicit def toSummaryOps(model: GraphModel): SummaryOps = model.summaryOps
}