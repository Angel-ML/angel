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

import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.{Partitioner, SparkPrivateClassProxy}
import scala.collection.mutable.ArrayBuffer

object Edge2dPartitionerFromPS {

  def getPsPartitionRange(psMatrix: PSMatrix): Array[(Long, Long)] = {
    val psPartitions = PSAgentContext.get.getMatrixMetaManager.getPartitions(psMatrix.id)
    var j = 0
    val psPartitionsRange = new ArrayBuffer[(Long, Long)]()
    while (j < psPartitions.size()) {
      val partitionKey = psPartitions.get(j)
      psPartitionsRange += ((partitionKey.getStartCol, partitionKey.getEndCol))
      j += 1
    }
    psPartitionsRange.toArray
  }

  def int2DPartitionerFromBounds(rangeBounds: Array[Int], dataPartitionNum: Int): Partitioner = {
    new Partitioner {
      override def numPartitions: Int = dataPartitionNum

      private val binarySearch: (Array[Int], Int) => Int = SparkPrivateClassProxy.makeBinarySearch[Int]

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
        require(key.isInstanceOf[(Int, Int)], s"Using 2D partition, the RDD key should be a (Int, Int) tuple")
        val k = key.asInstanceOf[(Int, Int)]
        val srcPsPartition = getPsPartition(k._1)
        val dstPsPartition = getPsPartition(k._2)
        PartitionStrategy.EdgePartition2D.getPartition(srcPsPartition, dstPsPartition, numPartitions)
      }
    }
  }

  def long2DPartitionerFromBounds(rangeBounds: Array[Long], dataPartitionNum: Int): Partitioner = {
    new Partitioner {
      override def numPartitions: Int = dataPartitionNum

      private val binarySearch: (Array[Long], Long) => Int = SparkPrivateClassProxy.makeBinarySearch[Long]

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
        require(key.isInstanceOf[(Long, Long)], s"Using 2D partition, the RDD key should be a (Int, Int) tuple")
        val k = key.asInstanceOf[(Long, Long)]
        val srcPsPartition = getPsPartition(k._1)
        val dstPsPartition = getPsPartition(k._2)
        PartitionStrategy.EdgePartition2D.getPartition(srcPsPartition, dstPsPartition, numPartitions)
      }
    }
  }

  def int2DPartitionerFromPS(psMatrix: PSMatrix, dataPartitionNum: Int, ascending: Boolean = true): Partitioner = {

    val psPartitionsRange = getPsPartitionRange(psMatrix)
    val arrayBoundsFromPS = psPartitionsRange.map(_._2.toInt - 1).slice(0, psPartitionsRange.length - 1)

    int2DPartitionerFromBounds(arrayBoundsFromPS, dataPartitionNum)
  }

  def long2DPartitionerFromPS(psMatrix: PSMatrix, dataPartitionNum: Int, ascending: Boolean = true): Partitioner = {

    val psPartitionsRange = getPsPartitionRange(psMatrix)
    val arrayBoundsFromPS = psPartitionsRange.map(_._2 - 1).slice(0, psPartitionsRange.length - 1)

    long2DPartitionerFromBounds(arrayBoundsFromPS, dataPartitionNum)
  }

}
