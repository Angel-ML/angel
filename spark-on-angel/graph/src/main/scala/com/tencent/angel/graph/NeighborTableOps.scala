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

import com.tencent.angel.graph.client.initneighbor2.{InitNeighbor => InitLongNeighbor, InitNeighborParam => InitLongNeighborParam}
import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.common.psf.param.LongKeysUpdateParam
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.vector.element.{ByteArrayElement, IElement, LongArrayElement}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.data._
import com.tencent.angel.graph.model.neighbor.simple.psf.init.InitNeighbors
import com.tencent.angel.graph.psf.triangle._
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.graph.utils.element.Element.VertexId
import com.tencent.angel.graph.utils.element.{NeighborTable, NeighborTablePartition}
import com.tencent.angel.spark.models.PSMatrix
import com.twitter.chill.ScalaKryoInstantiator
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class NeighborTableOps(table: NeighborTableModel) extends Serializable {

  def initLongNeighbor[ED: ClassTag](data: RDD[NeighborTablePartition[ED]]): NeighborTableModel = {
    // Neighbor table : a (1, maxIndex + 1) dimension matrix
    println(s"table.param.minIndex=${table.param.minIndex}, table.param.maxIndex=${table.param.maxIndex}, " +
      s"table.param.numNodes=${table.param.numNodes}")
    val modelContext = new ModelContext(table.param.psPartNum, table.param.minIndex, table.param.maxIndex,
      table.param.numNodes, "simple_neighbor", data.sparkContext.hadoopConfiguration)
    val mc = ModelContextUtils.createMatrixContext(modelContext, RowType.T_ANY_LONGKEY_SPARSE, classOf[ByteArrayElement])
    table.psMatrix  = PSMatrix.matrix(mc)

    data.foreach { part =>
      val size = part.size
      var pos = 0
      while (pos < size) {
        initLongNeighbor(table.psMatrix, part.takeBatch(pos, table.param.batchSize))
        pos += table.param.batchSize
      }
    }
    table
  }

  def initLongNeighborByteAttr(data: RDD[NeighborTablePartition[Byte]]): NeighborTableModel = {
    val mc: MatrixContext = new MatrixContext()
    mc.setName(table.neighborTableName)
    mc.setRowType(RowType.T_ANY_LONGKEY_SPARSE)
    mc.setRowNum(1)
    mc.setColNum(table.param.maxIndex)
    mc.setMaxColNumInBlock(table.param.maxIndex / table.param.psPartNum)
    // neighbor table compressed by Snappy
    mc.setValueType(classOf[NeighborsAttrsCompressedElement])

    table.psMatrix = PSMatrix.matrix(mc)

    data.foreach { part =>
      val size = part.size
      var pos = 0
      while (pos < size) {
        initLongNeighborByteAttr(table.psMatrix, part.takeBatch(pos, table.param.batchSize))
        pos += table.param.batchSize
      }
    }
    table
  }

  def initLongNeighborByteAttr(psMatrix: PSMatrix, pairs: Array[NeighborTable[Byte]]): NeighborTableModel = {
    val nodeId2Neighbors = new Long2ObjectOpenHashMap[NeighborsAttrsElement](pairs.length)
    pairs.foreach { item =>
      require(item.srcId < table.param.maxIndex, s"${item.srcId} exceeds the maximal node index ${table.param.maxIndex}")
      // put neighbor ids and attrs into TrianlgeCountElement
      val elem = new NeighborsAttrsElement(item.neighborIds, item.attrs)
      nodeId2Neighbors.put(item.srcId, elem)
    }

    val psFunc = new InitLongNeighborByteAttr(new InitLongNeighborByteAttrParam(psMatrix.id, nodeId2Neighbors))
    psMatrix.asyncPsfUpdate(psFunc).get()
    nodeId2Neighbors.clear()
    println(s"init ${pairs.length} long neighbors with attrs")
    table
  }

  def initLongNeighbor[ED: ClassTag](psMatrix: PSMatrix, pairs: Array[NeighborTable[ED]]): NeighborTableModel = {
    val nodeIds = new Array[Long](pairs.length)
    val neighborElems = new Array[IElement](pairs.length)
    var i = 0
    while (i < pairs.length) {
      val item = pairs(i)
      nodeIds(i) = item.srcId
      neighborElems(i) = new ByteArrayElement(ScalaKryoInstantiator.defaultPool.toBytesWithoutClass(item.neighborIds))
        .asInstanceOf[IElement]
      i += 1
    }
    psMatrix.psfUpdate(new InitNeighbors(new LongKeysUpdateParam(psMatrix.id, nodeIds, neighborElems)))
    println(s"init ${pairs.length} long neighbors")
    table
  }

  def getNeighborTable(nodeIds: Array[Int]): Int2ObjectOpenHashMap[Array[Int]] = {
    val neighborsMap = table.sampleNeighbors(nodeIds, -1)
    neighborsMap
  }

  def getLongNeighborTable(nodeIds: Array[Long]): Long2ObjectOpenHashMap[Array[Long]] = {
    val neighborsMap = table.sampleLongNeighbors(nodeIds, -1)
    neighborsMap
  }

  def getAttrLongNeighborTable[ED: ClassTag](nodeIds: Array[Long]): Long2ObjectOpenHashMap[Array[(Long, ED)]] = {
    val neighborsMap = table.getLongNeighborsByteAttrs(nodeIds)

    val psNeighborTable: Long2ObjectOpenHashMap[Array[(Long, ED)]] = new Long2ObjectOpenHashMap[Array[(Long, ED)]](nodeIds.length)
    for (nodeId <- nodeIds) {
      val elem = neighborsMap.get(nodeId)
      val arr = new ArrayBuffer[(Long, ED)](elem.getNumNodes)
      val edges: Array[Long] = elem.getNeighborIds
      val attrs: Array[ED] = elem.getAttrs.asInstanceOf[Array[ED]]

      for (i <- 0 until elem.getNumNodes) {
        arr += ((edges(i), attrs(i)))
      }
      psNeighborTable.put(nodeId, arr.toArray)
    }

    psNeighborTable
  }

  def checkpoint(): Unit = table.checkpoint()

  def testPS[ED: ClassTag](neighborsRDD: RDD[NeighborTablePartition[ED]],
                           num: Int = 10): NeighborTableModel = {
    val correct = neighborsRDD.map { part =>
      part.testPS(table, num)
    }.reduce(_ && _)
    assert(correct, "neighbor table is wrong")
    table
  }

  def calTriangleUndirected[ED: ClassTag](neighborsRDD: RDD[NeighborTablePartition[ED]], computeLCC: Boolean): RDD[(VertexId, Int, Float)] = {
    neighborsRDD.flatMap(_.calTriangleUndirected(table, computeLCC))
  }

  def calNumEdgesInNeighbor[ED: ClassTag](neighborsRDD: RDD[NeighborTablePartition[ED]]): RDD[
    (VertexId, Long, Seq[(VertexId, Long)])] = {
    neighborsRDD.flatMap(_.calNumEdgesInNeighbor(table))
  }

  def calNumEdgesInOutNeighbor[ED: ClassTag](neighborsRDD: RDD[NeighborTablePartition[ED]]): RDD[(VertexId, Long)] = {
    neighborsRDD.flatMap(_.calNumEdgesInOutNeighbor(table))
  }

}

object NeighborTableOps {

  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }
}
