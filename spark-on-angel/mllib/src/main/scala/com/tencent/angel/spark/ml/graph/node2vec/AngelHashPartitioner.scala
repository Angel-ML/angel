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
package com.tencent.angel.spark.ml.graph.node2vec

import org.apache.spark.Partitioner

class AngelHashPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case (src: Int, dst: Int) =>
      val key = if (src < dst) {
        (src.toLong << 32) + dst
      } else {
        (dst.toLong << 32) + src
      }

      nonNegativeMod(key, numPartitions)
    case (src: Long, dst: Long) =>
      val key = if (src < dst) {
        (src >> 1) + dst
      } else {
        (dst >> 1) + src
      }
      nonNegativeMod(key, numPartitions)
    case nodeId: Int => nonNegativeMod(nodeId, numPartitions)
    case nodeId: Long => nonNegativeMod(nodeId, numPartitions)
  }

  private def nonNegativeMod(x: Long, mod: Int): Int = {
    val rawMod = (x % mod).toInt
    rawMod + (if (rawMod < 0) mod else 0)
  }

  private def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  override def equals(other: Any): Boolean = other match {
    case h: AngelHashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}
