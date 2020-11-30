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

import com.tencent.angel.graph.client.initneighbor.{InitNeighbor, InitNeighborOver, InitNeighborOverParam, InitNeighborParam}
import com.tencent.angel.graph.client.initneighbor2.{InitNeighbor => InitLongNeighbor, InitNeighborParam => InitLongNeighborParam}
import com.tencent.angel.graph.client.sampleneighbor.{SampleNeighbor, SampleNeighborParam, SampleNeighborResult}
import com.tencent.angel.graph.client.sampleneighbor2.{SampleNeighbor => SampleLongNeighbor, SampleNeighborParam => SampleLongNeighborParam, SampleNeighborResult => SampleLongNeighborResult}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.partition.CSRPartition
import com.tencent.angel.ps.storage.vector.element.LongArrayElement
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.psf.triangle._
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer

/**
  * A simple neighbor table tool
  *
  * @param param neighbor table param
  */
class NeighborTableModel(@BeanProperty val param: Param) extends Serializable {

  val neighborTableName = "neighbor.table"
  var psMatrix: PSMatrix = _

  val ops = new NeighborTableOps(this)

  def initNeighbor(data: RDD[(Int, Int)]): NeighborTableModel = initNeighbor(data, param)

  def initNeighbor(data: RDD[(Int, Int)], param: Param): NeighborTableModel = {
    // Neighbor table : a (1, maxIndex + 1) dimension matrix
    val mc: MatrixContext = new MatrixContext(neighborTableName, 1, param.maxIndex + 1)
    mc.setRowType(RowType.T_INT_SPARSE)
    // Use CSR format to storage the neighbor table
    mc.setPartitionClass(classOf[CSRPartition])
    psMatrix = PSMatrix.matrix(mc)

    data.mapPartitions { iter =>
      // Init the neighbor table use many mini-batch to avoid big object
      iter.sliding(param.batchSize, param.batchSize).map(pairs => initNeighbors(psMatrix, pairs))
    }.count()

    // Merge the temp data to generate final neighbor table
    psMatrix.psfUpdate(new InitNeighborOver(new InitNeighborOverParam(psMatrix.id))).get()
    this
  }

  // TODO: optimize
  def initNeighbors(psMatrix: PSMatrix, pairs: Seq[(Int, Int)]): NeighborTableModel = {
    // Group By source node id
    val aggrResult = scala.collection.mutable.Map[Int, ArrayBuffer[Int]]()
    pairs.foreach(pair => {
      var neighbors: ArrayBuffer[Int] = aggrResult.get(pair._1) match {
        case None =>
          val temp = new ArrayBuffer[Int]()
          aggrResult += (pair._1 -> temp)
          temp
        case Some(x) => x
      }

      neighbors += pair._2
    })

    // Call initNeighbor psf to update neighbor table in PS
    val nodeIdToNeighbors = new Int2ObjectOpenHashMap[Array[Int]](aggrResult.size)
    aggrResult.foreach(nodeIdToNeighbor => nodeIdToNeighbors.put(nodeIdToNeighbor._1, nodeIdToNeighbor._2.toArray))
    aggrResult.clear()

    psMatrix.psfUpdate(new InitNeighbor(new InitNeighborParam(psMatrix.id, nodeIdToNeighbors))).get()
    this
  }

  /**
    * push the neighborsTable to Parameter Server
    *
    * @param data
    * @return
    */
  def initLongNeighbor(data: RDD[(Long, Array[Long])]): NeighborTableModel = {
    // Neighbor table : a (1, maxIndex + 1) dimension matrix
    val mc: MatrixContext = new MatrixContext()
    mc.setName(neighborTableName)
    mc.setRowType(RowType.T_ANY_LONGKEY_SPARSE)
    mc.setRowNum(1)
    mc.setColNum(param.maxIndex)
    mc.setMaxColNumInBlock(param.maxIndex / param.psPartNum)

    // the key's value is LongArrayElemnt type
    mc.setValueType(classOf[LongArrayElement])
    psMatrix = PSMatrix.matrix(mc)

    data.mapPartitions { iter => {
      // Init the neighbor table use many mini-batch to avoid big object
      iter.sliding(param.batchSize, param.batchSize).map(pairs => initLongNeighbors(psMatrix, pairs))
    }
    }.count()

    this
  }

  /**
    * push the mini-batch neighbors to ps
    *
    * @param psMatrix
    * @param pairs
    * @return
    */
  def initLongNeighbors(psMatrix: PSMatrix, pairs: Seq[(Long, Array[Long])]): NeighborTableModel = {
    val nodeIdToNeighbors = new Long2ObjectOpenHashMap[Array[Long]](pairs.length)
    pairs.foreach { case (src, neighbors) =>
      require(src < this.param.maxIndex, s"$src exceeds the maximal node index ${this.param.maxIndex}")
      nodeIdToNeighbors.put(src, neighbors)
    }
    // create psfunc
    val func = new InitLongNeighbor(new InitLongNeighborParam(psMatrix.id, nodeIdToNeighbors))
    // it means async push the nodes->neighbors to parameter server
    psMatrix.asyncPsfUpdate(func).get()
    nodeIdToNeighbors.clear()
    println(s"init ${pairs.length} long neighbors")
    this
  }

  def sampleNeighbors(nodeIds: Array[Int], count: Int): Int2ObjectOpenHashMap[Array[Int]] = {
    psMatrix.psfGet(new SampleNeighbor(new SampleNeighborParam(psMatrix.id, nodeIds, count)))
      .asInstanceOf[SampleNeighborResult].getNodeIdToNeighbors
  }

  def sampleLongNeighbors(nodeIds: Array[Long], count: Int): Long2ObjectOpenHashMap[Array[Long]] = {
    psMatrix.psfGet(new SampleLongNeighbor(new SampleLongNeighborParam(psMatrix.id, nodeIds, count)))
      .asInstanceOf[SampleLongNeighborResult].getNodeIdToNeighbors
  }

  def getLongNeighborsByteAttrs(nodeIds: Array[Long]): Long2ObjectOpenHashMap[NeighborsAttrsCompressedElement] = {
    psMatrix.psfGet(new GetNeighborWithByteAttr(new GetNeighborWithByteAttrParam(psMatrix.id, nodeIds)))
      .asInstanceOf[GetNeighborWithByteAttrResult].getNodeIdToNeighbors
  }

  def checkpoint(): Unit = {
    println(s"neighbor table checkpoint now matrixId=${psMatrix.id}")
    psMatrix.checkpoint()
  }
}

object NeighborTableModel {

  def apply(maxIndex: Long, batchSize: Int, pullBatch: Int, psPartNum: Int): NeighborTableModel = {
    val param = new Param(maxIndex, batchSize, pullBatch, psPartNum)
    new NeighborTableModel(param)
  }

  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }

  implicit def toOps(model: NeighborTableModel): NeighborTableOps = model.ops

}
