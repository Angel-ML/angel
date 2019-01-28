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

package com.tencent.angel.spark.ml.tree.util

import org.apache.spark.Partitioner

class EvenPartitioner(numKey: Int, numPartition: Int) extends Partitioner {
  private val numFeatPerPart = if (numKey % numPartition > numKey / 2) {
    Math.ceil(1.0 * numKey / numPartition).toInt
  } else {
    Math.floor(1.0 * numKey / numPartition).toInt
  }

  override def numPartitions: Int = numPartition

  override def getPartition(key: Any): Int = Math.min(key.asInstanceOf[Int] / numFeatPerPart, numPartition - 1)

  def partitionEdges(): Array[Int] = {
    val res = new Array[Int](numPartition + 1)
    for (i <- 0 until numPartition)
      res(i) = i * numFeatPerPart
    res(numPartition) = numKey
    res
  }
}

class IdenticalPartitioner(numPartition: Int) extends Partitioner {
  override def numPartitions: Int = numPartition

  override def getPartition(key: Any): Int = {
    val partId = key.asInstanceOf[Int]
    require(partId < numPartition, s"Partition id $partId exceeds maximum partition num $numPartition")
    partId
  }
}