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

package com.tencent.angel.graph.utils.partitionstrategy.reindexpartition

import java.util

import org.apache.spark.Partitioner

class IndexPartitioner(numParts: Int, rangeBounds: Array[Long]) extends Partitioner {

  override def numPartitions: Int = rangeBounds.length + 1

  private val binarySearch = makeBinarySearch()
  private val ordering = implicitly[Ordering[Long]]

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Long]
    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      partition = binarySearch(rangeBounds, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition-1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    partition
  }

  def makeBinarySearch() : (Array[Long], Long) => Int = {
    (l, x) => util.Arrays.binarySearch(l, x)
  }
}