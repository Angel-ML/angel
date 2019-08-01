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

package com.tencent.angel.spark.examples.graph

import java.util.Random

import com.tencent.angel.graph.client.initneighbor.{InitNeighbor, InitNeighborOver, InitNeighborOverParam, InitNeighborParam}
import com.tencent.angel.graph.client.sampleneighbor.{SampleNeighbor, SampleNeighborParam, SampleNeighborResult}
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.ps.storage.partition.CSRPartition
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * A simple neighbor table tool
  *
  * @param param neighbor table param
  */
class NeighborTable(param: Param) extends Serializable {
  val neighborTableName = "neighbor.table"
  var psMatrix: PSMatrix = _

  def initNeighbor(data: RDD[(Int, Int)], param: Param) = {
    // Neighbor table : a (1, maxIndex + 1) dimension matrix
    val mc: MatrixContext = new MatrixContext(neighborTableName, 1, param.maxIndex + 1)
    mc.setRowType(RowType.T_INT_SPARSE)
    // Use CSR format to storage the neighbor table
    mc.setPartitionClass(classOf[CSRPartition])
    psMatrix = PSMatrix.matrix(mc)

    var startTs = System.currentTimeMillis()
    data.mapPartitions {
      case iter => {
        // Init the neighbor table use many mini-batch to avoid big object
        iter.sliding(param.initBatchSize, param.initBatchSize).map(pairs => initNeighbors(psMatrix, pairs))
      }
    }.collect()
    val initTime = System.currentTimeMillis() - startTs

    // Merge the temp data to generate final neighbor table
    startTs = System.currentTimeMillis()
    psMatrix.psfUpdate(new InitNeighborOver(new InitNeighborOverParam(psMatrix.id))).get()
    println(s"init neighbor time = ${initTime} transfer to csr format use time ${System.currentTimeMillis() - startTs}")
    this
  }

  // TODO: optimize
  def initNeighbors(psMatrix: PSMatrix, pairs: Seq[(Int, Int)]) = {
    // Group By source node id
    var startTs = System.currentTimeMillis()
    val aggrResult = scala.collection.mutable.Map[Int, ArrayBuffer[Int]]()
    pairs.foreach(pair => {
      var neighbors: ArrayBuffer[Int] = aggrResult.get(pair._1) match {
        case None => {
          aggrResult += (pair._1 -> new ArrayBuffer[Int]())
          aggrResult.get(pair._1).get
        }
        case Some(x) => x
      }

      neighbors += pair._2
    })
    val groupByTime = System.currentTimeMillis() - startTs

    startTs = System.currentTimeMillis()
    // Call initNeighbor psf to update neighbor table in PS
    val nodeIdToNeighbors = new Int2ObjectOpenHashMap[Array[Int]](aggrResult.size)
    aggrResult.foreach(nodeIdToNeighbor => nodeIdToNeighbors.put(nodeIdToNeighbor._1, nodeIdToNeighbor._2.toArray))
    aggrResult.clear()

    psMatrix.psfUpdate(new InitNeighbor(new InitNeighborParam(psMatrix.id, nodeIdToNeighbors))).get()
    val pushTime = System.currentTimeMillis() - startTs
    println(s"groupByTime = ${groupByTime} pushTime = ${pushTime} node number = ${nodeIdToNeighbors.size()}")
    this
  }

  def sampleNeighborsTest(data: RDD[(Int, Int)], batchItemNum: Int, processNum: Int, count: Int): Unit = {
    data.mapPartitions {
      case iter => {
        // Init the neighbor table use many mini-batch to avoid big object
        iter.sliding(10, 10).map(pairs => {
          println(pairs.mkString(","))
          sampleNeighborsTest(batchItemNum, processNum, count)
        })
      }
    }.collect()
  }

  def sampleNeighborsTest(batchItemNum: Int, processNum: Int, count: Int): NeighborTable = {
    for (i <- (0 until processNum)) {
      var startTs = System.currentTimeMillis()
      val nodeIds = genIndexs(param.maxIndex, batchItemNum)
      val genNodeIdsTime = System.currentTimeMillis() - startTs

      startTs = System.currentTimeMillis()
      psMatrix.psfGet(new SampleNeighbor(new SampleNeighborParam(psMatrix.id, nodeIds, count)))
        .asInstanceOf[SampleNeighborResult].getNodeIdToNeighbors
      val getTime = System.currentTimeMillis() - startTs
      println(s"batch index = ${i} batchItemNum = ${batchItemNum} count = ${count} genNodeIdsTime=${genNodeIdsTime} getTime=${getTime}")

    }
    this
  }

  def genIndexs(feaNum: Int, nnz: Int): Array[Int] = {
    val sortedIndex = new Array[Int](nnz)
    val random = new Random(System.currentTimeMillis)
    sortedIndex(0) = random.nextInt(feaNum / nnz)
    for (i <- (1 until nnz)) {
      var rand = random.nextInt((feaNum - sortedIndex(i - 1)) / (nnz - i))
      if (rand == 0) rand = 1
      sortedIndex(i) = rand + sortedIndex(i - 1)
    }
    sortedIndex
  }

  // TODO: Optimize
  def sampleNeighbors(nodeIds: Array[Int], count: Int) = {
    psMatrix.psfGet(new SampleNeighbor(new SampleNeighborParam(psMatrix.id, nodeIds, count)))
      .asInstanceOf[SampleNeighborResult].getNodeIdToNeighbors
  }
}
