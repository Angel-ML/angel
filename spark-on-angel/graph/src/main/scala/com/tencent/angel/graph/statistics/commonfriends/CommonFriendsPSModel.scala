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

package com.tencent.angel.graph.statistics.commonfriends

import com.tencent.angel.graph.Param
import com.tencent.angel.graph.NeighborTableModel
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.rdd.RDD

/**
  * CommonFriendsPSModel use the neighborTableModel(a PSModel) to
  * init or get node's adjacency table (neighborTable)
  *
  * @param neighborTable
  */
class CommonFriendsPSModel(val neighborTable: NeighborTableModel) extends Serializable {

  def initNeighborTable(data: RDD[(Int, Int)]): Unit = {
    neighborTable.initNeighbor(data)
  }

  /**
    * init node neighborTable on ps
    *
    * @param data which store nodes,and its neighbors
    */
  def initLongNeighborTable(data: RDD[(Long, Array[Long])]): Unit = {
    neighborTable.initLongNeighbor(data)
  }

  def getNeighborTable(nodeIds: Array[Int]): Int2ObjectOpenHashMap[Array[Int]] = {
    val neighborsMap = neighborTable.sampleNeighbors(nodeIds, -1)
    neighborsMap
  }

  def getLongNeighborTable(nodeIds: Array[Long]): Long2ObjectOpenHashMap[Array[Long]] = {
    val neighborsMap = neighborTable.sampleLongNeighbors(nodeIds, -1)
    neighborsMap
  }

  def checkpoint(): Unit = {
    neighborTable.psMatrix.checkpoint()
  }
}

object CommonFriendsPSModel {

  def apply(maxIndex: Long, batchSize: Int, pullBatch: Int, psPartNum: Int): CommonFriendsPSModel = {
    val param = new Param(maxIndex, batchSize, pullBatch, psPartNum)
    val neighborTable = new NeighborTableModel(param)
    new CommonFriendsPSModel(neighborTable)
  }

}