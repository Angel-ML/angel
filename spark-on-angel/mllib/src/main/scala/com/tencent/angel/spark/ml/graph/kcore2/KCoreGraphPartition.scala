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
package com.tencent.angel.spark.ml.graph.kcore2

import java.util.{Arrays => JArrays}

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.LongIntVector
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList

private[kcore2] class KCoreGraphPartition(val keys: Array[Long],
                          val indptr: Array[Int],
                          val neighbours: Array[Long],
                          val maxDegree: Int) extends Serializable {

  val indices: Array[Long] = keys.union(neighbours).distinct
  val hindices: Array[Int] = new Array[Int](maxDegree)

  def init(model: KCorePSModel): Unit = {
    val update = VFactory.sparseLongKeyIntVector(model.dim)
    val func = Coder.withVersion(1)
    for (i <- keys.indices) {
      update.set(keys(i), func(indptr(i + 1) - indptr(i)))
    }
    model.update(update)
  }

  def process(model: KCorePSModel, version: Int, enable: Boolean = false): Int = {
    val coresWithVersions = model.pull(indices)
    val update = iteration(version, coresWithVersions, Coder.withVersion(version + 1))
    model.update(update)
    update.size().toInt
  }

  def iteration(version: Int, coresWithVersions: LongIntVector, withVersionFunc: Int => Int): LongIntVector = {
    val update = coresWithVersions.emptyLike()
    for (i <- keys.indices) {
      if (isActive(coresWithVersions, version, i)) {
        val curHindex = Coder.decodeCoreNumber(coresWithVersions.get(keys(i)))
        var j = indptr(i)
        while (j < indptr(i + 1)) {
          val n = neighbours(j)
          hindices(j - indptr(i)) = Coder.decodeCoreNumber(coresWithVersions.get(n))
          j += 1
        }
        val newHindex = calculateHIndex(hindices, 0, indptr(i + 1) - indptr(i))
        if (curHindex > newHindex)
          update.set(keys(i), withVersionFunc(newHindex))
      }
    }
    update
  }

  def isActive(coresWithVersions: LongIntVector, version: Int, index: Int): Boolean = {
    var j = indptr(index)
    while (j < indptr(index + 1)) {
      if (Coder.decodeVersion(coresWithVersions.get(neighbours(j))) >= version)
        return true
      j += 1
    }
    return false
  }

  def calculateHIndex(citations: Array[Int], from: Int, to: Int): Int = {
    JArrays.sort(citations, from, to)
    var i = to - 1
    var cnt = 1
    while (i >= from && citations(i) >= cnt) {
      cnt += 1
      i -= 1
    }
    cnt - 1
  }

  def resetVersion(model: KCorePSModel): Unit = {
    val cores = model.pull(keys.clone())
    val func = Coder.withNewVersion(1)
    for (idx <- keys.indices) {
      val value = cores.get(keys(idx))
      if (Coder.isMaxVersion(Coder.decodeVersion(value)))
        cores.set(keys(idx), func(value))
      else
        cores.set(keys(idx), Coder.decodeCoreNumber(value))
    }
    model.update(cores)
  }

  def save(model: KCorePSModel): (Array[Long], Array[Int]) = {
    (keys, model.pull(keys.clone()).get(keys).map(Coder.decodeCoreNumber))
  }

}


object KCoreGraphPartition {

  def apply(iterator: Iterator[(Long, Iterable[Long])]): KCoreGraphPartition = {
    val indptr = new IntArrayList()
    val keys = new LongArrayList()
    val neighbours = new LongArrayList()

    indptr.add(0)
    var maxDegree: Int = 0
    var size: Int = 0

    while (iterator.hasNext) {
      val entry = iterator.next()
      val (node, ns) = (entry._1, entry._2)
      ns.toArray.distinct.foreach(n => neighbours.add(n))
//      ns.foreach(n => neighbours.add(n))
      indptr.add(neighbours.size())
      keys.add(node)
      maxDegree = math.max(neighbours.size(), size)
      size = neighbours.size()
    }

    new KCoreGraphPartition(keys.toLongArray(), indptr.toIntArray(),
      neighbours.toLongArray(), maxDegree)
  }

}
