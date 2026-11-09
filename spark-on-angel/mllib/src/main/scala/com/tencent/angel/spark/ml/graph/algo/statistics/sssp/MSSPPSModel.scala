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
package com.tencent.angel.spark.ml.graph.algo.statistics.sssp

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{LongFloatVector, Vector}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.partitioner.ColumnRangePartitioner
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.psf.closeness.CalcCloseness
import com.tencent.angel.spark.models.PSMatrix
import com.tencent.angel.spark.models.impl.PSMatrixImpl
import org.apache.spark.rdd.RDD

private[sssp] class MSSPPSModel(val matrix: PSMatrix) extends Serializable {
  val dim: Int = matrix.columns.toInt
  val rows: Int = matrix.rows
  val numSources: Int = rows / 2

  private var baseVersion: Float = 1.0f
  private var numNodes: Long = 0
  private val distanceRowIndices: Array[Int] = (0 until rows by 2).toArray
  private val versionRowIndices: Array[Int] = (1 until rows by 2).toArray

  def init(sourceIds: Array[Long], allNodes: RDD[Long]): Unit = {
    assert(2 * sourceIds.length == matrix.rows)

    numNodes = allNodes.mapPartitions {
      iter =>
        var count = 0
        val initVects = sourceIds.map(_ => VFactory.sparseLongKeyFloatVector(dim))
        while (iter.hasNext) {
          val node = iter.next()
          initVects.foreach(f => f.set(node, Float.PositiveInfinity))
          count += 1
        }
        matrix.update((0 until rows by 2).toArray, initVects.map(_.asInstanceOf[Vector]))
        Iterator.single(count)
    }.reduce(_ + _)
    println(s"num nodes: $numNodes")

    val initVects = (0 until rows).toArray.map(_ => VFactory.sparseLongKeyFloatVector(dim))
    sourceIds.zipWithIndex.foreach {
      case (sourceId, idx) =>
        initVects(idx * 2).set(sourceId, 0.0f)
        initVects(idx * 2 + 1).set(sourceId, 1.0f)
    }
    matrix.update((0 until rows).toArray, initVects.map(_.asInstanceOf[Vector]))
  }

  def updateDistAndVersions(updateDist: Array[LongFloatVector],
                            updateVersions: Array[LongFloatVector],
                            activeSources: Array[Int]): Unit = {

    matrix.update(activeSources.map(_ * 2) ++ activeSources.map(_ * 2 + 1),
      (updateDist ++ updateVersions).map(_.asInstanceOf[Vector]))
  }

  def pull(nodes: Array[Long]): Array[LongFloatVector] = {
    val rows = (0 until matrix.rows).toArray
    matrix.pull(rows, nodes).map(f => f.asInstanceOf[LongFloatVector])
  }

  def pull(rowId: Int, nodes: Array[Long]): LongFloatVector =
    matrix.pull(rowId, nodes).asInstanceOf[LongFloatVector]


  def pullDistances(nodes: Array[Long]): Array[LongFloatVector] = {
    matrix.pull(distanceRowIndices, nodes).map(f => f.asInstanceOf[LongFloatVector])
  }

  def pullVersions(nodes: Array[Long]): Array[LongFloatVector] = {
    matrix.pull(versionRowIndices, nodes).map(f => f.asInstanceOf[LongFloatVector])
  }

  def calcCloseness(): Unit = {
    matrix.psfUpdate(new CalcCloseness(matrix.id, 0, rows, numNodes)).get()
//    matrix.pull(0).asInstanceOf[LongFloatVector]
  }

  def getBaseVersion: Float = baseVersion

  def resetVersion(): Unit = {
    baseVersion += 1
  }
}


private[sssp] object MSSPPSModel {

  def fromMaxId(numSources: Int, maxId: Long): MSSPPSModel = {
    // there are ``numSourceIds * 2`` rows in matrix
    // each pair of two rows represents the distance of current node from each sourceId, and the distance versions
    val matrix = new MatrixContext("mssp", 2 * numSources, 0, maxId)
    matrix.setRowType(RowType.T_FLOAT_SPARSE_LONGKEY)
    matrix.setPartitionerClass(classOf[ColumnRangePartitioner])
    matrix.setValidIndexNum(0)
    PSAgentContext.get().getMasterClient.createMatrix(matrix, 10000L)
    val matrixId = PSAgentContext.get().getMasterClient.getMatrix("mssp").getId
    new MSSPPSModel(new PSMatrixImpl(matrixId, matrix.getName, 2 * numSources, maxId, matrix.getRowType))
  }
}

