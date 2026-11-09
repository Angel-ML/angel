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
package com.tencent.angel.spark.ml.graph.utils.partitionstrategy.partitioners

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.{Partitioner, SparkPrivateClassProxy}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import com.tencent.angel.psagent.matrix.transport.router.hash.HashRouterUtils

object EdgeSquarePartitionerFromPS {

  def getPsPartitionRange(psMatrix: PSMatrix): Array[(Long, Long)] = {
    val psPartitions = PSAgentContext.get.getMatrixMetaManager.getPartitions(psMatrix.id)
    println(s"PS part num: ${psPartitions.size()}")
    var j = 0
    val psPartitionsRange = new ArrayBuffer[(Long, Long)]()
    while (j < psPartitions.size()) {
      val partitionKey = psPartitions.get(j)
      psPartitionsRange += ((partitionKey.getStartCol, partitionKey.getEndCol))
      j += 1
    }
    psPartitionsRange.toArray
  }

  def intSquarePartitionerFromBounds(rangeBounds: Array[Int], kernel: Int = 1): Partitioner = {
    new Partitioner {

      private val psPartNum: Int = rangeBounds.length + 1

      private val cols = if (psPartNum % kernel == 0) psPartNum / kernel else psPartNum / kernel + 1

      override def numPartitions: Int = cols * cols

      private val binarySearch: (Array[Int], Int) => Int = SparkPrivateClassProxy.makeBinarySearch[Int]

      private val map = {
        val map_ = new mutable.OpenHashMap[(Int, Int), Int]()
        val cols = if (psPartNum % kernel == 0) psPartNum / kernel else psPartNum / kernel + 1
        var i = 0
        while (i < psPartNum) {
          var j = 0
          while (j < psPartNum) {
            val row = i / kernel
            val col = j / kernel
            map_.put((i, j), row * cols + col)
            j += 1
          }
          i += 1
        }
        map_.map(x => x)
      }

      private def getPsPartition(key: Any, ascending: Boolean = true): Int = {
        val k = key.asInstanceOf[Int]
        var partition = 0
        if (rangeBounds.length <= 128) {
          // If we have less than 128 partitions naive search
          while (partition < rangeBounds.length && k > rangeBounds(partition)) {
            partition += 1
          }
        } else {
          // Determine which binary search method to use only once.
          partition = binarySearch(rangeBounds, k)
          // binarySearch either returns the match location or -[insertion point]-1
          if (partition < 0) {
            partition = -partition - 1
          }
          if (partition > rangeBounds.length) {
            partition = rangeBounds.length
          }
        }
        if (ascending) partition else rangeBounds.length - partition
      }

      override def getPartition(key: Any): Int = {
        require(key.isInstanceOf[(Int, Int)], s"Using 2D square partition, the RDD key should be a (Int, Int) tuple")
        val k = key.asInstanceOf[(Int, Int)]
        val srcPsPartition = getPsPartition(k._1)
        val dstPsPartition = getPsPartition(k._2)
        map((srcPsPartition, dstPsPartition))
      }
    }
  }

  def intSquarePartitionerFromPS(psMatrix: PSMatrix, modelContext: ModelContext, psPartitionNum: Int, kernel: Int = 2, ascending: Boolean = true): (Array[Int], Partitioner) = {

    if (modelContext.isUseRangePartition) {
      println("use range partitioner to generate edge partition")
      val psPartitionsRange = getPsPartitionRange(psMatrix)
      val arrayBoundsFromPS = psPartitionsRange.map(_._2.toInt - 1)

      (arrayBoundsFromPS, intSquarePartitionerFromBounds(arrayBoundsFromPS.init, kernel))
    } else {
      println("use hash partitioner to generate edge partition")
      
      (null, intSquarePartitionerFromHash(psPartitionNum, kernel))
    }
  }

  def intSquarePartitionerFromHash(partitionNum: Int, kernel: Int = 1): Partitioner = {
    new Partitioner {

      private val psPartNum = partitionNum

      private val cols = if (psPartNum % kernel == 0) psPartNum / kernel else psPartNum / kernel + 1

      override def numPartitions: Int = cols * cols

      private val map = {
        val map_ = new mutable.OpenHashMap[(Int, Int), Int]()
        val cols = if (psPartNum % kernel == 0) psPartNum / kernel else psPartNum / kernel + 1
        var i = 0
        while (i < psPartNum) {
          var j = 0
          while (j < psPartNum) {
            val row = i / kernel
            val col = j / kernel
            map_.put((i, j), row * cols + col)
            j += 1
          }
          i += 1
        }
        map_.map(x => x)
      }

      private def getPsPartition(key: Any): Int = {
        val k = key.asInstanceOf[Int]
        var hashCode: Int = Math.abs(k);   //hasher.hash(k)
        hashCode = hashCode ^ (hashCode >>> 16)
        if (hashCode < 0) hashCode -= Integer.MIN_VALUE

        if (HashRouterUtils.isPow2(partitionNum)) hashCode & (partitionNum - 1)
        else hashCode % partitionNum

      }

      override def getPartition(key: Any): Int = {
        require(key.isInstanceOf[(Int, Int)], s"Using 2D square partition, the RDD key should be a (Int, Int) tuple")
        val k = key.asInstanceOf[(Int, Int)]
        val srcPsPartition = getPsPartition(k._1)
        val dstPsPartition = getPsPartition(k._2)
        map((srcPsPartition, dstPsPartition))
      }
    }
  }

  def longSquarePartitionerFromBounds(rangeBounds: Array[Long], kernel: Int = 1): Partitioner = {
    new Partitioner {

      private val psPartNum: Int = rangeBounds.length + 1

      private val cols = if (psPartNum % kernel == 0) psPartNum / kernel else psPartNum / kernel + 1

      override def numPartitions: Int = cols * cols

      private val binarySearch: (Array[Long], Long) => Int = SparkPrivateClassProxy.makeBinarySearch[Long]

      private val map = {
        val map_ = new mutable.OpenHashMap[(Int, Int), Int]()
        val partIndex = Random.shuffle((0 until cols * cols).toList).toArray
        var i = 0
        while (i < psPartNum) {
          var j = 0
          while (j < psPartNum) {
            val row = i / kernel
            val col = j / kernel
            map_.put((i, j), partIndex(row * cols + col))
            j += 1
          }
          i += 1
        }
        map_.map(x => x)
      }

      private def getPsPartition(key: Any, ascending: Boolean = true): Int = {
        val k = key.asInstanceOf[Long]
        var partition = 0
        if (rangeBounds.length <= 128) {
          // If we have less than 128 partitions naive search
          while (partition < rangeBounds.length && k > rangeBounds(partition)) {
            partition += 1
          }
        } else {
          // Determine which binary search method to use only once.
          partition = binarySearch(rangeBounds, k)
          // binarySearch either returns the match location or -[insertion point]-1
          if (partition < 0) {
            partition = -partition - 1
          }
          if (partition > rangeBounds.length) {
            partition = rangeBounds.length
          }
        }
        if (ascending) partition else rangeBounds.length - partition
      }

      override def getPartition(key: Any): Int = {
        require(key.isInstanceOf[(Long, Long)], s"Using 2D square partition, the RDD key should be a (Long, Long) tuple")
        val k = key.asInstanceOf[(Long, Long)]
        val srcPsPartition = getPsPartition(k._1)
        val dstPsPartition = getPsPartition(k._2)
        map((srcPsPartition, dstPsPartition))
      }
    }
  }

  def longSquarePartitionerFromHash(partitionNum: Int, kernel: Int = 1): Partitioner = {
    new Partitioner {

      private val psPartNum: Int = partitionNum

      private val cols = if (psPartNum % kernel == 0) psPartNum / kernel else psPartNum / kernel + 1

      override def numPartitions: Int = cols * cols

      private val map = {
        val map_ = new mutable.OpenHashMap[(Int, Int), Int]()
        val partIndex = Random.shuffle((0 until cols * cols).toList).toArray
        var i = 0
        while (i < psPartNum) {
          var j = 0
          while (j < psPartNum) {
            val row = i / kernel
            val col = j / kernel
            map_.put((i, j), partIndex(row * cols + col))
            j += 1
          }
          i += 1
        }
        map_.map(x => x)
      }

      private def getPsPartition(key: Any, ascending: Boolean = true): Int = {
        val k = key.asInstanceOf[Long]
        var hashCode: Int = Math.abs((k ^ (k >>> 32)).toInt)
        hashCode = hashCode ^ (hashCode >>> 16)
        if (hashCode < 0) hashCode -= Integer.MIN_VALUE

        if (HashRouterUtils.isPow2(partitionNum)) hashCode & (partitionNum - 1)
        else hashCode % partitionNum
      }

      override def getPartition(key: Any): Int = {
        require(key.isInstanceOf[(Long, Long)], s"Using 2D square partition, the RDD key should be a (Long, Long) tuple")
        val k = key.asInstanceOf[(Long, Long)]
        val srcPsPartition = getPsPartition(k._1)
        val dstPsPartition = getPsPartition(k._2)
        map((srcPsPartition, dstPsPartition))
      }
    }
  }

  def longSquarePartitionerFromPS(psMatrix: PSMatrix, modelContext: ModelContext, psPartitionNum: Int, kernel: Int = 2, ascending: Boolean = true): Partitioner = {
    if (modelContext.isUseRangePartition) {
      println("use range partitioner to generate edge partition")
      val psPartitionsRange = getPsPartitionRange(psMatrix)
      val arrayBoundsFromPS = psPartitionsRange.map(_._2 - 1)

      longSquarePartitionerFromBounds(arrayBoundsFromPS.init, kernel)
    } else {
      println("use hash partitioner to generate edge partition")

      longSquarePartitionerFromHash(psPartitionNum, kernel)
    }
  }

  def main(Args: Array[String]): Unit = {
    val psPartNum = 5
    val kernel = 2
    val cols = if (psPartNum % kernel == 0) psPartNum / kernel else psPartNum / kernel + 1
    val map = {
      val map_ = new mutable.OpenHashMap[(Int, Int), Int]()
      var i = 0
      while (i < psPartNum) {
        var j = 0
        while (j < psPartNum) {
          val row = i / kernel
          val col = j / kernel
          map_.put((i, j), row * cols + col)
          j += 1
        }
        i += 1
      }
      map_
    }
    map.toArray.sortBy(_._2).foreach(println)
  }


}