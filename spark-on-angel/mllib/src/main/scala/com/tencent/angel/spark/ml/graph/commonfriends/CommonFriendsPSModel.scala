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

package com.tencent.angel.spark.ml.graph.commonfriends

import com.tencent.angel.spark.ml.graph.Param
import com.tencent.angel.spark.ml.graph.NeighborTable
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.apache.spark.rdd.RDD

class CommonFriendsPSModel(val neighborTable: NeighborTable) extends Serializable {

  def initNeighborTable(data: RDD[(Int, Int)]): Unit = {
    neighborTable.initNeighbor(data)
  }

  def getNeighborTable(nodeIds: Array[Int]): Int2ObjectOpenHashMap[Array[Int]] = {
    val neighborsMap = neighborTable.sampleNeighbors(nodeIds, -1)
    neighborsMap
  }
}

object CommonFriendsPSModel {

  def apply(maxIndex: Int, batchSize: Int, psPartNum: Int): CommonFriendsPSModel = {
    val param = new Param(maxIndex, batchSize, psPartNum)
    val neighborTable = new NeighborTable(param)
    new CommonFriendsPSModel(neighborTable)
  }

}