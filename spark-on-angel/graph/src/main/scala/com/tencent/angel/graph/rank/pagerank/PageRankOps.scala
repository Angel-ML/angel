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

package com.tencent.angel.graph.rank.pagerank

import java.util.Collections

import com.tencent.angel.psagent.PSAgentContext

object PageRankOps {

  def summarizeApplyOp(iterator: Iterator[(Long, Long, Float)]): Iterator[(Long, Long, Long)] = {
    var minId = Long.MaxValue
    var maxId = Long.MinValue
    var numEdges = 0
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (src, dst) = (entry._1, entry._2)
      minId = math.min(minId, src)
      minId = math.min(minId, dst)
      maxId = math.max(maxId, src)
      maxId = math.max(maxId, dst)
      numEdges += 1
    }

    Iterator.single((minId, maxId, numEdges))
  }

  def summarizeReduceOp(t1: (Long, Long, Long),
                        t2: (Long, Long, Long)): (Long, Long, Long) =
    (math.min(t1._1, t2._1), math.max(t1._2, t2._2), t1._3 + t2._3)

  def splitPartitionIds(matrixId: Int, partitionNum: Int): (Array[Int], Array[Int]) = {
    val parts = PSAgentContext.get().getMatrixMetaManager.getPartitions(matrixId)
    Collections.shuffle(parts)

    val length = parts.size()
    val sizes = new Array[Int](partitionNum)
    for (i <- sizes.indices)
      sizes(i) = length / sizes.length
    for (i <- 0 until (length % sizes.length))
      sizes(i) += 1

    for (i <- 1 until sizes.length)
      sizes(i) += sizes(i - 1)

    val partitionIds = new Array[Int](length)
    for (i <- 0 until length)
      partitionIds(i) = parts.get(i).getPartitionId

    (partitionIds, sizes)
  }

  def save(index: Int, model: PageRankModel, partitionIds: Array[Int],
           ends: Array[Int], numBatch: Int): Iterator[(Array[Long], Array[Float])] = {
    if (index < ends.length) {
      val length = if (index > 0) ends(index) - ends(index - 1) else ends(0)
      val start = if (index > 0) ends(index - 1) else 0
      val myPartitionIds = new Array[Int](length)
      for (i <- start until ends(index))
        myPartitionIds(i - start) = partitionIds(i)

      if (length > 0) {
        val nodesIterator = model.getNodes(myPartitionIds, numBatch)
        val iterator = new Iterator[(Array[Long], Array[Float])] with Serializable {
          def hasNext: Boolean = nodesIterator.hasNext

          def next: (Array[Long], Array[Float]) = {
            val nodes = nodesIterator.next()
            val ranks = model.readRanks(nodes.clone()).get(nodes)
            (nodes, ranks)
          }
        }
        return iterator
      }
    }

    return Iterator.empty
  }

}
