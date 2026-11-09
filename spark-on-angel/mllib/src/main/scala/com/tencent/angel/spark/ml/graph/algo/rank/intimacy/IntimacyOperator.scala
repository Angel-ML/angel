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
package com.tencent.angel.spark.ml.graph.algo.rank.intimacy
import com.tencent.angel.spark.ml.graph.utils.BatchIter
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import com.tencent.angel.spark.ml.graph.utils.collection.OpenHashMap

import scala.collection.mutable

object IntimacyOperator {

  def runNeighborPartition(partitionId: Int, iter: Iterator[(Long, Array[(Long, Float)])],
                           psModel: IntimacyPSModel, pullBatchSize: Int, top: Int):
  Iterator[(Long, String)] = {
    val batchSize = pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"partition $partitionId: last batch cost ${System.currentTimeMillis() - startTs} ms")
      startTs = System.currentTimeMillis()
      var numSrcNodes = 0
      val pullNodes: mutable.HashSet[Long] = new mutable.HashSet[Long]()
      val localNeighborTable = new Long2ObjectOpenHashMap[Array[(Long, Float)]](batchSize)
      batchIter.foreach { case (src, neighbors) =>
        numSrcNodes += 1
        localNeighborTable.put(src, neighbors)
        if (localNeighborTable.containsKey(src))
          pullNodes ++= neighbors.map(_._1)
      }
      val beforePullTs = System.currentTimeMillis()
      val psNeighborsTable = psModel.getLongNeighborAttrs(pullNodes.toArray)
      totalRowNum += numSrcNodes
      totalPullNum += pullNodes.size
      println(s"partition $partitionId: process $numSrcNodes neighbor tables " +
        s"($totalRowNum in total), " +
        s"pull ${pullNodes.size} nodes from ps ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      val srcNodes = localNeighborTable.keySet().toLongArray
      srcNodes.map { src =>
        val srcNeighbors = localNeighborTable.get(src)
        val srcNeighborsSet = srcNeighbors.map(_._1).toSet
        val values = srcNeighbors.flatMap { case (dst, attr) =>
          val secondNeisAttrs = psNeighborsTable.get(dst)
          val secondNeis = secondNeisAttrs.getNeighborIds
          val secondAttrs = secondNeisAttrs.getAttrs
          secondNeis.zip(secondAttrs).filter(x => !srcNeighborsSet.contains(x._1))
            .map { case (node, w) => (node, w * attr) }
        }.groupBy(_._1).map(x => (x._1, x._2.map(_._2).sum))
          .toArray.sortWith(_._2 > _._2).take(top)
        (src, values.map(x => x._1 + ":" + x._2).mkString(" "))
      }
    }
  }

  def runNeighborPartition_(partitionId: Int, iter: Iterator[(Long, Array[(Long, Float)])],
                           psModel: IntimacyPSModel, pullBatchSize: Int, top: Int):
  Iterator[(Long, Array[(Long, Float)])] = {
    val batchSize = pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"partition $partitionId: last batch cost ${System.currentTimeMillis() - startTs} ms")
      startTs = System.currentTimeMillis()
      var numSrcNodes = 0
      val pullNodes: mutable.HashSet[Long] = new mutable.HashSet[Long]()
      val localNeighborTable = new Long2ObjectOpenHashMap[Array[(Long, Float)]](batchSize)
      batchIter.foreach { case (src, neighbors) =>
        numSrcNodes += 1
        localNeighborTable.put(src, neighbors)
        if (localNeighborTable.containsKey(src))
          pullNodes ++= neighbors.map(_._1)
      }
      val beforePullTs = System.currentTimeMillis()
      val psNeighborsTable = psModel.getLongNeighborAttrs(pullNodes.toArray)
      totalRowNum += numSrcNodes
      totalPullNum += pullNodes.size
      println(s"partition $partitionId: process $numSrcNodes neighbor tables " +
        s"($totalRowNum in total), " +
        s"pull ${pullNodes.size} nodes from ps ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      val srcNodes = localNeighborTable.keySet().toLongArray
      srcNodes.flatMap { src =>
        val srcNeighbors = localNeighborTable.get(src)
        val srcNeighborsSet = srcNeighbors.map(_._1).toSet
        val intimacy = new OpenHashMap[Long, Float]()
        srcNeighbors.foreach { case (dst, attr) =>
          val secondNeisAttrs = psNeighborsTable.get(dst)
          val secondNeis = secondNeisAttrs.getNeighborIds
          val secondAttrs = secondNeisAttrs.getAttrs
          secondNeis.zip(secondAttrs).foreach { case (node, w) =>
            if (!(srcNeighborsSet.contains(node) || node == src)) {
              intimacy.changeValue(node, w * attr, x => x + w * attr)
            }
          }
        }
        if (intimacy.nonEmpty) {
          val re = intimacy.toArray.sortWith(_._2 > _._2).take(top)
          Iterator.single(src, re)
        } else
          Iterator.empty
      }
    }
  }
}
