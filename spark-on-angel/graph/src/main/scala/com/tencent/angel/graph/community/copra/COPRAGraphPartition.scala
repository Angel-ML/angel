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
package com.tencent.angel.graph.community.copra


import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.{Long2ObjectOpenHashMap, LongArrayList}
import com.tencent.angel.graph.psf.triangle.NeighborsFloatAttrsElement
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class COPRAGraphPartition(index: Int,
                          numMaxCommunities: Int,
                          preserveRate: Float,
                          keys: Array[Long],
                          indptr: Array[Int],
                          neighbors: Array[Long],
                          keyLabels: Array[Array[(Long, Float)]],
                          indices: Array[Long], weights: Array[Float],
                          batchSize: Int) {

  def analysis(): Int = neighbors.length

  def initMsgs(model: COPRAPSModel): Unit = {
    keys.sliding(batchSize, batchSize).foreach {partKeys =>
      initPartMsgs(partKeys, model)}
  }

  def initPartMsgs(partKeys: Array[Long], model: COPRAPSModel): Unit = {
    val nodeIdToComs = new Long2ObjectOpenHashMap[NeighborsFloatAttrsElement](partKeys.length)
    for (i <- partKeys.indices) {
      nodeIdToComs.put(partKeys(i), new NeighborsFloatAttrsElement(Array(partKeys(i)), Array(1.0f)))
    }
    model.initComInMsgs(nodeIdToComs)
  }

  def process(model: COPRAPSModel, iteration: Int): COPRAGraphPartition = {
    println(s"partition $index: ---------- iteration $iteration starts ----------")
    keys.indices.sliding(batchSize, batchSize).foreach { nodesIndex =>
      val beforeCalcPullNodesTs = System.currentTimeMillis()
      val pullNodes = neighbors.slice(indptr(nodesIndex.head), indptr(nodesIndex.last+1)).distinct
      println(s"partition $index: calculating pull nodes cost ${System.currentTimeMillis() - beforeCalcPullNodesTs} ms")

      val beforePullCoeTs = System.currentTimeMillis()
      val comInMsgs = model.readComInMsgs(pullNodes)
      println(s"partition $index: pull ${pullNodes.length} nodes from ps, cost ${System.currentTimeMillis() - beforePullCoeTs} ms")

      val beforeComputeTs = System.currentTimeMillis()
      val comOutMsgs = new Long2ObjectOpenHashMap[NeighborsFloatAttrsElement](indices.length)

      for (idx <- nodesIndex) {
        val newLabel = if (Random.nextDouble() < preserveRate) {
          if (keyLabels(idx) == null) {
            keyLabels(idx) = Array((keys(idx), 1f))
          }
          keyLabels(idx)
        } else {
          calcLabel(idx, comInMsgs)
        }
        val (newL, newC) = newLabel.unzip
        comOutMsgs.put(keys(idx), new NeighborsFloatAttrsElement(newL, newC))
        keyLabels(idx) = newLabel
      }
      println(s"partition $index: " +
        s"compute ${nodesIndex.length} nodes, " +
        s"cost ${System.currentTimeMillis() - beforeComputeTs} ms")

      val beforePushTs = System.currentTimeMillis()
      model.writeComOutMsgs(comOutMsgs)
      println(s"partition $index: " +
        s"push ${nodesIndex.length} nodes to ps, " +
        s"cost ${System.currentTimeMillis() - beforePushTs} ms")


    }
    println(s"partition $index: ---------- iteration $iteration terminated ----------")

    new COPRAGraphPartition(index, numMaxCommunities, preserveRate, keys, indptr, neighbors, keyLabels, indices, weights, batchSize)
  }

  def calcLabel(idx: Int, comInMsgs: Long2ObjectOpenHashMap[NeighborsFloatAttrsElement]): Array[(Long, Float)] = {
    var j = indptr(idx)
    //    val temp = mutable.Map(-1L -> 0f)
    val temp = new mutable.OpenHashMap[Long, Float]()
    while (j < indptr(idx + 1)) {
      assert(comInMsgs.containsKey(neighbors(j)), s"Key ${neighbors(j)} is not in the keySet of comInMsgs.")
      val comGet = comInMsgs.get(neighbors(j))
      val neis = comGet.getNeighborIds
      val coes = comGet.getAttrs
      var x = 0
      while (x < comGet.getNodesNum) {
        val label = neis(x)
        val coe = coes(x)
        val t = temp.getOrElse(label, 0f)
        temp += ((label, t + coe * weights(j)))
        x += 1
      }
      j += 1
    }
    //    temp.remove(-1)
    if (temp.isEmpty) {
      println(s"aggregated result for key ${keys(idx)} is empty. Use the label from the last iteration.")
      keyLabels(idx)
    } else {
      val threshold = temp.values.sum / numMaxCommunities
      val top_k = temp.toArray.sortWith(_._2 > _._2).take(numMaxCommunities)
      assert(top_k.nonEmpty, s"top_k is empty for key ${keys(idx)}")
      val newAttr = mutable.Map(top_k.head)
      top_k.tail.filter(_._2 >= threshold).foreach { case (node, coe) =>
        newAttr(node) = coe
      }
      // normalize
      val norm = newAttr.values.sum
      newAttr.map { case (node, coe) => (node, coe / norm)}.toArray
    }
  }

  def save(): (Array[Long], Array[Array[(Long, Float)]]) =
    (keys, keyLabels)
}

object COPRAGraphPartition {

  def apply(index: Int, iterator: Iterator[(Long, Iterable[(Long, Float)])], numMaxCommunities: Int, preserveRate: Float, batchSize: Int): COPRAGraphPartition = {

    //    val beLongingCoe = 1.0 / numMaxCommunities
    val indptr = new IntArrayList()
    val keys = new LongArrayList()
    val neighbors = new LongArrayList()
    val keyLabels = ArrayBuffer[Array[(Long,Float)]]()
    val weights = new FloatArrayList()

    indptr.add(0)
    var j = 0
    while (iterator.hasNext) {
      val (nodes, ns) = iterator.next()
      ns.toArray.distinct.sorted.foreach { case (n, w) =>
        neighbors.add(n)
        weights.add(w)
      }
      indptr.add(neighbors.size())
      keys.add(nodes)
      val lb = Array((nodes, 1f))
      keyLabels += lb
      j += 1
    }

    val keysArray = keys.toLongArray()
    val neighborsArray = neighbors.toLongArray()
    val indicesArray = keysArray.union(neighborsArray).distinct

    new COPRAGraphPartition(index, numMaxCommunities,preserveRate,
      keysArray,
      indptr.toIntArray(),
      neighborsArray,
      keyLabels.toArray,
      indicesArray,
      weights.toFloatArray(),
      batchSize)
  }
}
