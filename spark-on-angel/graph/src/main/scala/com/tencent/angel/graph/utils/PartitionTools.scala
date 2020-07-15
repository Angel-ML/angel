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

package com.tencent.angel.graph.utils

import com.tencent.angel.ml.matrix.{MatrixContext, PartContext}
import org.apache.spark.graphx.PartitionStrategy
import org.apache.spark.{Partitioner, SparkPrivateClassProxy}

import scala.collection.JavaConversions._

object PartitionTools {

  def addPartition(ctx: MatrixContext, bounds: Array[Long]): Unit = {
    var lower = Long.MinValue
    var numPart = 0
    for (upper <- bounds ++ Array(Long.MaxValue)) {
      if (lower < upper) {
        ctx.addPart(new PartContext(0, 1, lower, upper, -1))
        lower = upper
        numPart += 1
      }
    }
    println(s"add numPart=$numPart: ${ctx.getParts().map(_.toString).mkString("\n")}")
  }

  def rangePartitionerFromBounds(rangeBounds: Array[Long], ascending: Boolean = true): Partitioner = {

    new Partitioner {
      private val binarySearch: (Array[Long], Long) => Int = SparkPrivateClassProxy.makeBinarySearch[Long]

      override def numPartitions: Int = {
        rangeBounds.length + 1
      }

      override def getPartition(key: Any): Int = {
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
        if (ascending) {
          partition
        } else {
          rangeBounds.length - partition
        }
      }
    }
  }

  def rangePartitioner(maxIdx: Int, numPartition: Int, ascending: Boolean = true): Partitioner = {

    new Partitioner {
      println(s"new range partitioner, max node index = $maxIdx, partition number = $numPartition")

      private val binarySearch: (Array[Int], Int) => Int = SparkPrivateClassProxy.makeBinarySearch[Int]

      override def numPartitions: Int = {
        numPartition
      }

      def lengthPerPart: Int = maxIdx / numPartition

      def lengthLeft: Int = maxIdx % numPartition

      lazy val splits: Array[Int] = {
        val lengths = Array.fill(numPartition)(lengthPerPart)
        (0 until lengthLeft).foreach { idx =>
          lengths(idx) + 1
        }
        lengths.scanLeft(0)(_ + _).tail
      }

      println(s"range partition splits: ${splits.mkString(",")}")

      override def getPartition(key: Any): Int = {
        val k = key.asInstanceOf[Int]
        var partition = 0
        if (numPartition <= 128) {
          while (partition < numPartition && k >= splits(partition)) {
            partition += 1
          }
        } else {
          // Determine which binary search method to use only once.
          partition = binarySearch(splits, k)
          // binarySearch either returns the match location or -[insertion point]-1
          if (partition < 0) {
            partition = -partition - 1
          }
          if (partition >= splits.length) {
            partition = splits.length - 1
          }
        }
        if (ascending) {
          partition
        } else {
          splits.length - partition
        }
      }
    }
  }

  def edge2DPartitioner(numPartition: Int): Partitioner = {

    new Partitioner {
      override def numPartitions: Int = numPartition

      override def getPartition(key: Any): Int = {
        require(key.isInstanceOf[(Long, Long)], s"Using 2D partition, the RDD key should be a (Long, Long) tuple")
        val k = key.asInstanceOf[(Long, Long)]
        PartitionStrategy.EdgePartition2D.getPartition(k._1, k._2, numPartitions)
      }
    }
  }

}
