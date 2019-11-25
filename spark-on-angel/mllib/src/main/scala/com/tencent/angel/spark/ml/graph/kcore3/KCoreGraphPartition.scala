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
package com.tencent.angel.spark.ml.graph.kcore3

import com.tencent.angel.ml.math2.VFactory
import it.unimi.dsi.fastutil.ints.{IntArrayList, IntOpenHashSet}
import it.unimi.dsi.fastutil.longs.{LongArrayList, LongOpenHashSet}

private[kcore3] class KCoreGraphPartition(var keys: Array[Long],
                                          var indptr: Array[Int],
                                          var neighbours: Array[Long]) extends Serializable {
  val copy = keys.clone()
  def init(model: KCorePSModel): Unit = {
    val update = VFactory.sparseLongKeyIntVector(model.dim)
    for (i <- keys.indices)
      update.set(keys(i), indptr(i + 1) - indptr(i))
    model.updateDegree(update)
  }

  def getIndices(): Array[Long] = {
    val set = new LongOpenHashSet()
    neighbours.foreach(f => set.add(f))
    set.toLongArray()
  }

  def process(model: KCorePSModel, K: Int): (Int, Int) = {
//    println(s"keys.length=${keys.length} indptr.length=${indptr.length}")
    val indices = getIndices()
    val degrees = model.pullDegree(indices)
    val kcoreUpdate = degrees.emptyLike()
    val degreeUpdate = degrees.emptyLike()

    val newKeys = new LongArrayList()
    val newIndptr = new IntArrayList()
    val newNeighbours = new LongArrayList()

    newIndptr.add(0)
    for (i <- keys.indices) {
      if (indptr(i + 1) - indptr(i) <= K) {
        kcoreUpdate.set(keys(i), K)
      } else {
        newKeys.add(keys(i))
        var j = indptr(i)
        var cnt = 0
        while (j < indptr(i + 1)) {
          if (degrees.get(neighbours(j)) > K) {
            newNeighbours.add(neighbours(j))
            cnt += 1
          }
          j += 1
        }
        newIndptr.add(newNeighbours.size())
        if (cnt != indptr(i + 1) - indptr(i))
          degreeUpdate.set(keys(i), cnt)
      }
    }

//    println(s"keys.length=${keys.length} indptr.length=${indptr.length}")

    keys = newKeys.toLongArray()
    indptr = newIndptr.toIntArray()
    neighbours = newNeighbours.toLongArray()

    model.updateDegreeAndKcore(degreeUpdate, kcoreUpdate)
    (degreeUpdate.size().toInt, keys.length)
//    new KCoreGraphPartition(newKeys.toLongArray(),
//      newIndptr.toIntArray(),
//      newNeighbours.toLongArray())
  }

  def save(model: KCorePSModel): (Array[Long], Array[Int]) = {
    (copy, model.pullKcore(copy.clone()).get(copy))
  }

}

object KCoreGraphPartition {

  def apply(iterator: Iterator[(Long, Iterable[Long])]): KCoreGraphPartition = {
    val indptr = new IntArrayList()
    val keys = new LongArrayList()
    val neighbours = new LongArrayList()

    indptr.add(0)
    var size: Int = 0

    while (iterator.hasNext) {
      val entry = iterator.next()
      val (node, ns) = (entry._1, entry._2)
      ns.toArray.distinct.foreach(n => neighbours.add(n))
      indptr.add(neighbours.size())
      keys.add(node)
      size = neighbours.size()
    }

    new KCoreGraphPartition(keys.toLongArray(), indptr.toIntArray(),
      neighbours.toLongArray())
  }

}
