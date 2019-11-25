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
package com.tencent.angel.spark.ml.graph.kcore4

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.storage.LongIntSparseVectorStorage
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.{Long2IntOpenHashMap, LongArrayList, LongOpenHashSet}

private[kcore4] class KCoreGraphPartition(var keys: Array[Long],
                                          var indptr: Array[Int],
                                          var neighbours: Array[Long]) extends Serializable {

  val copy = keys.clone()
  val flags = new Array[Boolean](keys.length)
  java.util.Arrays.fill(flags, true)

  def init(model: KCorePSModel): Unit = {
    val degree = VFactory.sparseLongKeyIntVector(model.dim)
    val version = degree.emptyLike()

    for (i <- keys.indices) {
      degree.set(keys(i), indptr(i + 1) - indptr(i))
      version.set(keys(i), 0)
    }

    model.updateDegreeAndVersion(degree, version)
  }

  def getIndices(): Array[Long] = {
    val set = new LongOpenHashSet()
    keys.foreach(f => set.add(f))
    neighbours.foreach(f => set.add(f))
    set.toLongArray()
  }

  def reduceEdge(model: KCorePSModel, K: Int): Int = {
    val degrees = model.pullDegree(getIndices())
    val newKeys = new LongArrayList()
    val newIndptr = new IntArrayList()
    val newNeighbours = new LongArrayList()

    newIndptr.add(0)
    for (i <- keys.indices) {
      if (degrees.get(keys(i)) > K) {
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
      }
    }

    keys = newKeys.toLongArray()
//    println(s"reduce edges: keys=${keys.mkString(",")}")
    indptr = newIndptr.toIntArray()
    neighbours = newNeighbours.toLongArray()
    java.util.Arrays.fill(flags, true)
    keys.length
  }

  def reduceDegree(model: KCorePSModel, version: Int, K: Int): Int = {
    val (degrees, versions) = model.pullDegreeAndVersion(keys.clone())
    val updateDegree = new Long2IntOpenHashMap()
    val updateVersion = versions.emptyLike()
    val updateKcore = versions.emptyLike()
    for (i <- keys.indices) {
//      println(s"key=${keys(i)} flag=${flags(i)} deg=${degrees.get(keys(i))}")
      if (flags(i) && degrees.get(keys(i)) <= K) {
        updateKcore.set(keys(i), K)
//        println(s"core ${keys(i)} K=$K")
      }

      if (flags(i) && versions.get(keys(i)) >= version && degrees.get(keys(i)) <= K) {
        var j = indptr(i)
        while (j < indptr(i + 1)) {
          val n = neighbours(j)
          updateDegree.addTo(n, -1)
          updateVersion.set(n, version + 1)
          j += 1
        }
        flags(i) = false
      }
    }

    degrees.setStorage(new LongIntSparseVectorStorage(degrees.dim(), updateDegree))
    model.addDegree(degrees)
    model.updateVersionAndKcore(updateVersion, updateKcore)
    updateVersion.size().toInt
  }

  def start(model: KCorePSModel, version: Int = 1, K: Int = 1): Int = {
    val degreeUpdate = new Long2IntOpenHashMap()
    val kcoreUpdate = VFactory.sparseLongKeyIntVector(model.dim)
    val versionUpdate = kcoreUpdate.emptyLike()
    for (i <- keys.indices) {
//      println(s"key=${keys(i)} flag=${flags(i)} deg=${indptr(i+1) - indptr(i)}")
      if (indptr(i + 1) - indptr(i) <= K && flags(i)) {
        kcoreUpdate.set(keys(i), K)
//        println(s"core ${keys(i)} K=$K")
        flags(i) = false
        var j = indptr(i)
        while (j < indptr(i + 1)) {
          degreeUpdate.addTo(neighbours(j), -1)
          versionUpdate.set(neighbours(j), version + 1)
          j += 1
        }
      }
    }
    val update = kcoreUpdate.emptyLike()
    update.setStorage(new LongIntSparseVectorStorage(update.dim(), degreeUpdate))
    model.addDegree(update)
    model.updateVersionAndKcore(versionUpdate, kcoreUpdate)
    versionUpdate.size().toInt
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