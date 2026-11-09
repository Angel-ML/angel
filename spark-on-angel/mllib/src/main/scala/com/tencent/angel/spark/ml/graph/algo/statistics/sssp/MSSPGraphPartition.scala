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
package com.tencent.angel.spark.ml.graph.algo.statistics.sssp

import com.tencent.angel.ml.math2.vector.LongFloatVector
import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.{LongArrayList, LongOpenHashSet}

class MSSPGraphPartition(keys: Array[Long],
                         indptr: Array[Int],
                         outNodes: Array[Long],
                         weights: Array[Float]) extends Serializable {

  assert(keys.length == indptr.length - 1)

  def activeKeys(model: MSSPPSModel): (Array[Array[Int]], Array[Long]) = {
    val baseVersion = model.getBaseVersion
    val versions = model.pullVersions(keys.clone())
    val actives = (0 until model.numSources).map(_ => new IntArrayList())
    val set = new LongOpenHashSet()

    var i = 0
    while (i < keys.length) {
      for (src <- 0 until model.numSources) {
        if (versions(src).get(keys(i)) == baseVersion) {
          set.add(keys(i))
          actives(src).add(i)
          var j = indptr(i)
          while (j < indptr(i + 1)) {
            set.add(outNodes(j))
            j += 1
          }
        }
      }
      i += 1
    }
    (actives.map(_.toIntArray()).toArray,
      set.toLongArray)
  }

  def process(model: MSSPPSModel, srcNumModified: Array[Int]) : (Array[Int], Int) = {
    val (activeIndex, indices) = activeKeys(model)
    if (indices.length == 0) return (Array.fill(model.numSources)(0), 0)

    val matrix = model.pull(indices.clone())
    val baseVersion = model.getBaseVersion

    val activeSources = srcNumModified.indices.filter(srcNumModified(_) != 0).toArray
    var numModified = Array.fill(model.numSources)(0)
    val (outDists, outVersions, _) = activeSources.map{
      src =>
        val tuple3 = mssp(activeIndex(src),
          matrix(src * 2),
          matrix(src * 2 + 1),
          baseVersion)

        numModified(src) = tuple3._3
        tuple3
    }.unzip3

    model.updateDistAndVersions(outDists, outVersions, activeSources)
    (numModified, indices.length)
  }

  def mssp(activeIndex: Array[Int],
           dists: LongFloatVector,
           versions: LongFloatVector,
           baseVersion: Float): (LongFloatVector, LongFloatVector, Int) = {
    val outDist = dists.emptyLike()
    val outVersions = versions.emptyLike()

    val w = weights
    for (idx <- activeIndex) {
      var j = indptr(idx)
      val d = dists.get(keys(idx))
      while (j < indptr(idx + 1)) {
        val outNode = outNodes(j)
        if (d + w(j) < dists.get(outNode)) {
          outVersions.set(outNode, baseVersion + 1)
          outDist.set(outNode, d + w(j))
          dists.set(outNode, d + w(j))
        }
        j += 1
      }
    }

    (outDist, outVersions, outVersions.size().toInt)
  }

  def save(model: MSSPPSModel): (Array[Long], Array[Array[Float]]) = {
    val dists = model.pullDistances(keys.clone())
    (keys, dists.map(_.get(keys)))
  }

}

private[sssp] object MSSPGraphPartition {
  def apply(iterator: Iterator[(Long, Iterable[(Long, Float)])]): MSSPGraphPartition = {
    val indptr = new IntArrayList()
    val outNodes = new LongArrayList()
    val keys = new LongArrayList()
    val weights = new FloatArrayList()

    indptr.add(0)
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (node, outs) = (entry._1, entry._2)
      outs.foreach { case (out, weight) =>
        outNodes.add(out)
        weights.add(weight)
      }
      indptr.add(outNodes.size())
      keys.add(node)
    }

    new MSSPGraphPartition(keys.toLongArray(), indptr.toIntArray(),
      outNodes.toLongArray(), weights.toFloatArray())
  }
}
