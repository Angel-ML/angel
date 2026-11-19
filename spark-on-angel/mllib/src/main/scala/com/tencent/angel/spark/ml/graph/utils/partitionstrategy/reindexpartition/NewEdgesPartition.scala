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
package com.tencent.angel.spark.ml.graph.utils.partitionstrategy.reindexpartition

import java.util.{Arrays => JArrays}

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.LongIntVector
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList


class NewEdgesPartition(index: Int,
                        srcs: Array[Long],
                        dsts: Array[Long],
                        indices: Array[Long]) extends Serializable {

  def process(model: PSModel): Int = {
    val msgs = model.readMsgs(indices)
//    val msgs = model.readAllMsgs()
    for (i <- srcs.indices) {
      srcs(i) = msgs.get(srcs(i))
      dsts(i) = msgs.get(dsts(i))
    }
    0
  }

  def save(): (Array[Long], Array[Long]) =
    (srcs, dsts)
}


object NewEdgesPartition {
  def apply(index: Int, edges: Iterator[(Long, Long)]): NewEdgesPartition = {

    val src = new LongArrayList()
    val dst = new LongArrayList()

    while (edges.hasNext) {
      val entry = edges.next()
      val (s, d) = (entry._1, entry._2)
      src.add(s)
      dst.add(d)
    }

    val srcArray = src.toLongArray()
    val dstArray = dst.toLongArray()
    val indices = srcArray.union(dstArray).distinct

    new NewEdgesPartition(index, srcArray, dstArray, indices)
  }

}
