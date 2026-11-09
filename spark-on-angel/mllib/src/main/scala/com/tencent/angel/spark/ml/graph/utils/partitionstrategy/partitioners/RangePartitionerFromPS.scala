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
import org.apache.spark.{Partitioner, SparkPrivateClassProxy}
import scala.collection.mutable.ArrayBuffer

object RangePartitionerFromPS {

  def getPsPartitionRange(psMatrix: PSMatrix): Array[(Int, Int)] = {
    val psPartitions = PSAgentContext.get.getMatrixMetaManager.getPartitions(psMatrix.id)
    var j = 0
    val psPartitionsRange = new ArrayBuffer[(Int, Int)]()
    while (j < psPartitions.size()) {
      val partitionKey = psPartitions.get(j)
      psPartitionsRange += ((partitionKey.getStartCol.toInt, partitionKey.getEndCol.toInt))
      j += 1
    }
    psPartitionsRange.toArray
  }

  def intRangePartitionerFromBounds(rangeBounds: Array[Int], ascending: Boolean = true, times: Int = 1): Partitioner = {
    new Partitioner {
      private val binarySearch: (Array[Int], Int) => Int = SparkPrivateClassProxy.makeBinarySearch[Int]

      override def numPartitions: Int = {
        rangeBounds.length + 1
      }

      private val psPartNum = numPartitions / times

      private val map = {
        (0 until numPartitions).toArray.map { x =>
          val rest = x % times
          val multiple = x / times
          (x, rest * psPartNum + multiple)
        }
      }.toMap

      override def getPartition(key: Any): Int = {
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
        val temp = if (ascending) partition else rangeBounds.length - partition
        val part = map(temp)
        part
      }
    }
  }

  def intRangePartitionerFromPS(psMatrix: PSMatrix, dataPartitionNum: Int, ascending: Boolean = true): Partitioner = {

    val psPartitionsRange = getPsPartitionRange(psMatrix)
    val psPartNum = psPartitionsRange.length
    if (psPartitionsRange.length >= dataPartitionNum) {
      val arrayBoundsFromPS = psPartitionsRange.map(_._2 - 1).slice(0, psPartitionsRange.length - 1)
      println(s"dataPartitionNum < psPartitionNum ($dataPartitionNum vs $psPartNum), reset data partition num to $psPartNum.")
      intRangePartitionerFromBounds(arrayBoundsFromPS)
    } else { // cut psArrayBounds to dataPartitionNum slices
      var times = dataPartitionNum / psPartNum
      val rest = dataPartitionNum % psPartNum
      // to ensure data load balance, set the real data partition num to be a multiple of ps partition num.
      val dataPartNum = if (rest.toFloat / psPartNum < 0.3) {
        psPartNum * times
      } else {
        times += 1
        psPartNum * times
      }
      println(s"dataPartitionNum vs psPartitionNum ($dataPartitionNum vs $psPartNum), reset data partition num to $dataPartNum.")

      // cut ps partition range
      val newPartitionRange = new ArrayBuffer[(Int, Int)]()
      for (i <- psPartitionsRange.indices) {
        val (startCol, endCol) = psPartitionsRange(i)
        val partLoad = (endCol - startCol) / times
        var lastStartCol = startCol
        for (j <- 1 until times) {
          val tempEndCol = lastStartCol + partLoad
          newPartitionRange.append((lastStartCol, tempEndCol))
          lastStartCol = tempEndCol
        }
        newPartitionRange.append((lastStartCol, endCol))
      }
      val arrayBoundsFromPS = newPartitionRange.map(_._2 - 1).slice(0, newPartitionRange.length - 1).toArray
      intRangePartitionerFromBounds(arrayBoundsFromPS, times = times)
    }
  }

}