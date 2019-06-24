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

package com.tencent.angel.spark.ml.graph

import com.tencent.angel.spark.ml.{PSFunSuite, SharedPSContext}
import it.unimi.dsi.fastutil.ints.{Int2ObjectMap}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.Random

/**
  * A simple example of neighbor table
  */
class InitGraphTest extends PSFunSuite with SharedPSContext {
  val input = "../../data/bc/edge"
  val output = "model/"
  val numPartition = 4
  val initBatchSize = 1024000
  val psPartNum = 100
  val storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  var param: Param = _
  var data: RDD[(Int, Int)] = _
  var neighborTable:NeighborTable = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    data = sc.textFile(input).mapPartitions { iter =>
      val r = new Random()
      iter.map { line =>
        val arr = line.split(" ")
        val src = arr(0).toInt
        val dst = arr(1).toInt
        (r.nextInt, (src, dst))
      }
    }.repartition(numPartition).values.persist(storageLevel)
    val numEdge = data.count()
    val maxNodeId = data.map { case (src, dst) => math.max(src, dst) }.max().toLong + 1
    println(s"numEdge=$numEdge maxNodeId=$maxNodeId")

    /*data = sc.textFile(input).mapPartitions { iter =>
      iter.map {
        line => {
          val arr = line.split(" ")
          val src = arr(0).toInt
          val dst = arr(1).toInt
          (src -> dst)
        }
      }
    }*/

    //data.groupBy()

    param = new Param()
    param.initBatchSize = initBatchSize
    param.maxIndex = maxNodeId.toInt
    param.psPartNum = psPartNum

    neighborTable = new NeighborTable(param)
  }

  test("init neighbors") {
    // Init neighbor table
    neighborTable.initNeighbor(data, param)

    // Sample neighbors for some nodes
    val nodeIds = new Array[Int](1)
    nodeIds(0) = 5988
    val nodeIdToNeighbors = neighborTable.sampleNeighbors(nodeIds, -1)
    val iter = nodeIdToNeighbors.int2ObjectEntrySet().fastIterator()
    while(iter.hasNext) {
      printNeighbors(iter.next())
    }
  }

  def printNeighbors(v:Int2ObjectMap.Entry[Array[Int]]) = {
    println(s"node id = ${v.getIntKey}, neighbor len = ${v.getValue.size} neighbors = ${v.getValue.mkString(",")}")
  }
}
