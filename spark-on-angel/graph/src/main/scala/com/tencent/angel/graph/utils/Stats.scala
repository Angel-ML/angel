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

import org.apache.spark.rdd.RDD

object Stats {
  def summarize(edge: RDD[(Long, Long)]): (Long, Long, Long) = {
    edge.mapPartitions(summarizeApplyOp).reduce(summarizeReduceOp)
  }

  def summarizeWithWeight(edge: RDD[(Long, Long, Float)]): (Long, Long, Long) = {
    edge.mapPartitions(summarizeApplyOp1).reduce(summarizeReduceOp)
  }

  def summarizeApplyOp(iterator: Iterator[(Long, Long)]): Iterator[(Long, Long, Long)] = {
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

    Iterator.single((minId, maxId + 1, numEdges))
  }

  def summarizeApplyOp1(iterator: Iterator[(Long, Long, Float)]): Iterator[(Long, Long, Long)] = {
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

    Iterator.single((minId, maxId + 1, numEdges))
  }

  def summarizeReduceOp(t1: (Long, Long, Long),
                        t2: (Long, Long, Long)): (Long, Long, Long) =
    (math.min(t1._1, t2._1), math.max(t1._2, t2._2), t1._3 + t2._3)
}