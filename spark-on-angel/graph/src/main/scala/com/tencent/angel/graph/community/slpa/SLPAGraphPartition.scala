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

package com.tencent.angel.graph.community.slpa

import com.tencent.angel.graph.psf.triangle.NeighborsFloatAttrsElement
import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.{Long2ObjectOpenHashMap, LongArrayList}
import org.apache.commons.math3.distribution.EnumeratedDistribution
import org.apache.commons.math3.util.Pair

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class SLPAGraphPartition(index: Int,
                         preserveRate: Float,
                         srcNodesArray: Array[Long],
                         indptr: Array[Int],
                         neighbors: Array[Long],
                         srcNodesLabels: Array[Array[(Long, Float)]],
                         indices: Array[Long],
                         weights: Array[Float],
                         batchSize: Int) {
  def analysis(): Int = neighbors.length

  /**
   * init node's lable with node id
   *
   * @param model the ps model
   */
  def initMsgs(model: SLPAPSModel): Unit = {
    srcNodesArray.sliding(batchSize, batchSize).foreach { partKeys =>
      initPartMsgs(partKeys, model)
    }
  }


  /**
   * init part node's lable with node id
   *
   * @param partKeys part node key
   * @param model    the ps model
   */
  def initPartMsgs(partKeys: Array[Long], model: SLPAPSModel): Unit = {
    val nodeIdToComs = new Long2ObjectOpenHashMap[NeighborsFloatAttrsElement](partKeys.length)
    for (i <- partKeys.indices) {
      nodeIdToComs.put(partKeys(i), new NeighborsFloatAttrsElement(Array(partKeys(i)), Array(1.0f)))
    }
    model.initComInMsgs(nodeIdToComs)
  }

  /**
   * the label propagation process
   *
   * @param model     the ps model
   * @param iteration the iter number
   * @return new SLPAGraphPartition object
   */
  def process(model: SLPAPSModel, iteration: Int): SLPAGraphPartition = {
    println(s"partition $index: ---------- iteration $iteration starts ----------")
    //computer node new label in batch
    srcNodesArray.indices.sliding(batchSize, batchSize).foreach { nodesIndex =>
      val pullNodes = neighbors.slice(indptr(nodesIndex.head), indptr(nodesIndex.last + 1)).distinct
      val comInMsgs = model.readComInMsgs(pullNodes)
      val comOutMsgs = new Long2ObjectOpenHashMap[NeighborsFloatAttrsElement](indices.length)

      //computer each node
      for (idx <- nodesIndex) {
        //val degree = indptr(idx+1)-indptr(idx)
        val newLabel = if (Random.nextDouble() < preserveRate) {
          if (srcNodesLabels(idx) == null) {
            srcNodesLabels(idx) = Array((srcNodesArray(idx), 1f))
          }
          srcNodesLabels(idx)
        } else {
          //compute node label by listener and speaker rule
          speakerListenerCalculate(idx, comInMsgs)
        }
        val (newL, newC) = newLabel.unzip
        comOutMsgs.put(srcNodesArray(idx), new NeighborsFloatAttrsElement(newL, newC))
        srcNodesLabels(idx) = newLabel
      }
      model.writeComOutMsgs(comOutMsgs)
    }
    println(s"partition $index: ---------- iteration $iteration terminated ----------")
    new SLPAGraphPartition(index, preserveRate, srcNodesArray, indptr, neighbors, srcNodesLabels, indices, weights, batchSize)
  }

  /**
   * the speaker and listener process
   *
   * @param idx       the node index
   * @param comInMsgs the neighbors' community labels
   * @return the new src community label array
   */
  def speakerListenerCalculate(idx: Int, comInMsgs: Long2ObjectOpenHashMap[NeighborsFloatAttrsElement]): Array[(Long, Float)] = {
    var j = indptr(idx)
    val temp = new mutable.OpenHashMap[Long, Float]() //record sample labels and coefficients from speaker
    val srcNodesLabelsMap = mutable.Map[Long, Float]() //record old labels and coefficients from listener
    srcNodesLabels(idx).foreach(x => srcNodesLabelsMap(x._1) = x._2)

    //compute each neighbor of the srcNode
    while (j < indptr(idx + 1)) {
      assert(comInMsgs.containsKey(neighbors(j)), s"Key ${neighbors(j)} is not in the keySet of comInMsgs.")
      val comGet = comInMsgs.get(neighbors(j))
      if (comGet != null) {
        val neis = comGet.getNeighborIds //the labels list of neighbor j
        val coes = comGet.getAttrs //the label coefficients list of neighbor j
        var x = 0
        //create Pair(Label,coefficient)
        val memorySpeakerList = new java.util.ArrayList[Pair[Float, java.lang.Double]]()
        while (x < comGet.getNodesNum) {
          val label = neis(x)
          val coe = coes(x).toDouble
          memorySpeakerList.add(new Pair(label, coe))
          x += 1
        }

        if (!memorySpeakerList.isEmpty) {
          //sample one label from label list of neighbor j
          val memorySpeakerTemp = new EnumeratedDistribution(memorySpeakerList)
          val sampleFromSpearker = memorySpeakerTemp.sample().toLong
          val t = temp.getOrElse(sampleFromSpearker, 0.0f)
          temp += ((sampleFromSpearker, t + weights(j)))
        }
      }
      j += 1
    }

    if (temp.isEmpty) {
      println(s"aggregated result for key ${srcNodesArray(idx)} is empty. Use the label from the last iteration.")
      srcNodesLabels(idx)
    } else {
      //get the label with max coefficient from all sample labels
      val (maxLabel, maxCoe) = temp.maxBy(a => a._2)
      val tt = srcNodesLabelsMap.getOrElse(maxLabel, 0f)
      srcNodesLabelsMap += ((maxLabel, tt + 1.0f)) //1.0 or maxCoe
      srcNodesLabelsMap.toArray
    }

  }


  /**
   * return the node list and community label list
   *
   * @return the node list and community label list
   */
  def save(): (Array[Long], Array[Array[(Long, Float)]]) =
    (srcNodesArray, srcNodesLabels)
}


object SLPAGraphPartition {

  /**
   * rebuild the graph data in one partition to the SLPAGraphPartition class
   *
   * @param index        the partition inidex
   * @param iterator     the node adjacency list
   * @param preserveRate maintain node's label of the possibility
   * @param batchSize    the compute unit
   * @return the SLPAGraphPartition object
   */
  def apply(index: Int, iterator: Iterator[(Long, Iterable[(Long, Float)])], preserveRate: Float, batchSize: Int): SLPAGraphPartition = {

    val indptr = new IntArrayList()
    val srcNodes = new LongArrayList()
    val neighbors = new LongArrayList()
    val srcNodesLabels = ArrayBuffer[Array[(Long, Float)]]()
    val weights = new FloatArrayList()

    indptr.add(0)
    while (iterator.hasNext) {
      val (nodes, ns) = iterator.next()
      ns.toArray.distinct.sorted.foreach { case (n, w) =>
        neighbors.add(n)
        weights.add(w)
      }
      indptr.add(neighbors.size())
      srcNodes.add(nodes)
      val lb = Array((nodes, 1f))
      srcNodesLabels += lb
    }

    val srcNodesArray = srcNodes.toLongArray()
    val neighborsArray = neighbors.toLongArray()
    val indicesArray = srcNodesArray.union(neighborsArray).distinct

    new SLPAGraphPartition(index,
      preserveRate,
      srcNodesArray,
      indptr.toIntArray(),
      neighborsArray,
      srcNodesLabels.toArray,
      indicesArray,
      weights.toFloatArray(),
      batchSize)
  }
}
