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
package com.tencent.angel.graph.community.leiden

import com.tencent.angel.spark.ml.util.Utils._
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap
import org.apache.spark.SparkPrivateClassProxy

import scala.beans.BeanProperty
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

private[leiden] class CSR extends Serializable {
  @BeanProperty var id: Int = 0
  @BeanProperty var superNodes: Array[Long] = _
  @BeanProperty var nodePros: Array[Float] = _
  @BeanProperty var tightness: Array[Float] = _
  @BeanProperty var indexes: Array[Int] = _
  @BeanProperty var edges: Array[Long] = _
  @BeanProperty var edgePros: Array[Float] = _

  @BeanProperty var min: Long = Long.MinValue
  @BeanProperty var max: Long = Long.MaxValue

  @BeanProperty var queue: mutable.Queue[Int] = new mutable.Queue[Int]()
  @BeanProperty var batchSize: Int = 2


  def this(id: Int,
           nodes: Array[Long],
           nodePros: Array[Float],
           innerEdgePros: Array[Float],
           indexes: Array[Int],
           edges: Array[Long],
           edgePros: Array[Float],
           batchSize: Int) {
    this()
    this.id = id
    this.superNodes = nodes
    this.nodePros = nodePros
    this.tightness = innerEdgePros
    this.indexes = indexes
    this.edges = edges
    this.edgePros = edgePros
    this.superNodes.indices.foreach(this.queue.enqueue(_))
    this.batchSize = batchSize
  }

  def getEdges(i: Int): Array[Long] = this.edges.slice(this.indexes(i), this.indexes(i + 1))

  def getNode(i: Int): Long = this.superNodes(i)

  def getNodePro(i: Int): Float = this.nodePros(i)

  def getNodeEdgePro(i: Int, j: Int): Float = this.edgePros(this.indexes(i) + j)

  def isInnerComm(comm: Long): Boolean = comm >= this.min && comm <= this.max

  def getEdgePros(i: Int): Array[Float] = this.edgePros.slice(this.indexes(i), this.indexes(i + 1))

  def getComm(candidates: Array[((Long, Float), Float)]): Option[(Long, Float)] = {
    val length = candidates.length
    if (length > 0 && candidates(length - 1)._2 > 0) {
      val ((c1, size1), f1) = candidates(length - 1)
      if (length > 1 && (candidates(length - 2)._2 == candidates(length - 1)._2)) {
        val ((c2, size2), f2) = candidates(length - 2)
        if (this.isInnerComm(c2)) { // to inner
          Some((c2, f2))
        } else if (this.isInnerComm(c1)) { // to inner
          Some((c1, f1))
        } else { // to ghost which more vertex
          if (size1 > size2) {
            Some((c1, f1))
          } else {
            Some((c2, f2))
          }
        }
      } else {
        Some((c1, f1))
      }
    } else {
      Option.empty[(Long, Float)]
    }
  }

  private def moveOnPerBatch(model: LeidenPSModel, newQueue: ArrayBuffer[Long], gamma: Float): this.type = {
    var batchIndexes = mutable.Queue[Int]()
    var count = 0
    while (this.queue.nonEmpty && count < batchSize) {
      batchIndexes.enqueue(this.queue.dequeue())
      count += 1
    }

    batchIndexes = Random.shuffle(batchIndexes)

    //println(s"[$id]batch:[${batchIndexes.map(this.getNode).mkString(", ")}]")
    val sources = batchIndexes.map(this.getNode).toArray
    val allNeighbors = batchIndexes.flatMap(i => this.getEdges(i)).distinct.toArray
    val allNodes = (allNeighbors ++ sources).distinct
    val (node2comm, comm2Nodes, comm2Edges) = model.getModelPart(allNodes)

    val updateComm = new Long2LongOpenHashMap()

    val commInfoMap = SparkPrivateClassProxy.createOpenHashMap[Long, (WeightMarker, WeightMarker)]()
    comm2Nodes.getStorage.entryIterator().asScala.map(_.getLongKey).toArray
      .zip(comm2Edges.getStorage.getValues.zip(comm2Nodes.getStorage.getValues))
      .foreach { case (c, (edgeWeight, nodeWeight)) =>
        commInfoMap.changeValue(c, (WeightMarker(edgeWeight, edgeWeight),
          WeightMarker(nodeWeight, nodeWeight)), x => x)
      }

    val node2visitedInfo = SparkPrivateClassProxy.createOpenHashMap[Long, Long]()
    model.getNode2Visited(allNodes)
      .getStorage
      .entryIterator()
      .asScala
      .foreach { entry => node2visitedInfo.changeValue(entry.getLongKey, entry.getLongValue, x => x) }

    while (batchIndexes.nonEmpty) {
      val i = batchIndexes.dequeue()
      val source = this.getNode(i)
      val sourceComm = node2comm.get(source)
      val sourceSize = this.getNodePro(i)

      //println(s"[$id]source:$source sourceComm:$sourceComm sourceSize:$sourceSize")

      val nbrs = this.getEdges(i).map(nbr => nbr -> updateComm.getOrDefault(nbr, node2comm.get(nbr)))

      val targetCommEdges = SparkPrivateClassProxy.createOpenHashMap[Long, Float]()
      var sourceCommEdges = 0.0F

      for (j <- nbrs.indices) {
        val (target, targetComm) = nbrs(j)
        val edgeWeight = this.getNodeEdgePro(i, j)
        if (sourceComm != targetComm) {
          targetCommEdges.changeValue(targetComm, edgeWeight, value => value + edgeWeight)
        } else {
          sourceCommEdges += edgeWeight
        }
        //println(s"  [$id]nbr:$target targetComm:$targetComm ")
      }

      val candidates = targetCommEdges.map { case (cm, targetEdges) =>
        val targetSize = commInfoMap(cm)._2.last
        val delta = CPM.delta(targetEdges, sourceCommEdges, gamma, sourceSize, targetSize, commInfoMap(sourceComm)._2.last)
        //println(s"  [$id] $source-$sourceComm => $cm: $delta=$targetEdges-$sourceCommEdges-$gamma*$sourceSize*($sourceSize+$targetSize-${commInfoMap(sourceComm)._2.last})")
        (cm, targetSize) -> delta
      }.toArray

      if (candidates.nonEmpty) {
        val candidate = this.getComm(candidates.sortBy(_._2))
        if (candidate.nonEmpty) {
          val (newComm, delta) = candidate.get
          if (newComm != sourceComm) {
            val sourceInfo = commInfoMap(sourceComm)
            val targetInfo = commInfoMap(newComm)
            // old/new community info
            sourceInfo._1.setLast(sourceInfo._1.last - sourceCommEdges)
            targetInfo._1.setLast(targetInfo._1.last + targetCommEdges(newComm))
            sourceInfo._2.setLast(sourceInfo._2.last - sourceSize)
            targetInfo._2.setLast(targetInfo._2.last + sourceSize)

            //  Identify neighbors of v that are not in newComm, but visited before
            nbrs.foreach { case (target, targetComm) =>
              if (targetComm != newComm) {
                node2visitedInfo.changeValue(target, 0, info => {
                  if (info > 0) newQueue.append(target)
                  0
                })
              }
            }
            //println(s"  [$id]$source-$sourceComm into $newComm  $delta queque:[${newQueue.mkString(", ")}]")
            // update source community id
            updateComm.put(source, newComm)
          }
        }
      }
      node2visitedInfo.changeValue(source, 0, _ => 1)
    }

    // update ps
    val tmp = commInfoMap.map { case (comm, (e, v)) => comm -> (e.change, v.change) }.toArray
    val edgeTotal = tmp.map { case (comm, (e, _)) => comm -> e }.filter(_._2 != 0).unzip
    val nodeTotal = tmp.map { case (comm, (_, v)) => comm -> v }.filter(_._2 != 0).unzip
    model.incrementComm2edges(edgeTotal._1, edgeTotal._2)
    model.incrementComm2nodes(nodeTotal._1, nodeTotal._2)

    val (nodes, comms) = updateComm.asScala.toArray.map(x => x._1.toLong -> x._2.toLong).unzip
    model.updateNode2community(nodes, comms)

    model.updateNode2Visited(sources, Array.fill(sources.length)(1))
    this
  }

  def moveNodesOnBatch(gamma: Float, model: LeidenPSModel): this.type = {
    val newQueue = new ArrayBuffer[Long]()
    val total = this.queue.size
    val edges = this.queue.map(i => this.getEdges(i).length).sum
    val interval = getBatchInterval(total, batchSize, 20)
    var time = 0
    while (this.queue.nonEmpty) {
      time += 1
      val current = this.queue.size
      val (_, cost) = runOnTimer(() => moveOnPerBatch(model, newQueue, gamma))
      if (time % interval == 0 || this.queue.isEmpty) {
        val finished = 100 * ((total - this.queue.size) / total.toFloat)
        println(s"[graph:$id v:$total e:$edges] move ${current - this.queue.size} nodes cost ${secondTimer(cost)}s," +
          s" process ${finished.formatted("%.2f")}%.")
      }
    }

    if (newQueue.nonEmpty) {
      def func(batch: Array[Long]) = model.updateRankNodeStatus(batch, Array.fill(batch.length)(1))

      iteratorBatch(newQueue.iterator, func, batchSize)
    }
    this
  }

  def aggregateOnBatch(model: LeidenPSModel): Iterator[((Long, Long), Float)] = {
    this.makeBatchIterator().flatMap { case (start, end) =>
      val indexes = Iterator.range(start, end).toArray
      val sources = indexes.map(this.getNode)
      val nodes = (indexes.flatMap(i => this.getEdges(i)).distinct ++ sources).distinct
      val node2comm = model.getNode2Comm(nodes)
      indexes.flatMap { i =>
        val srcComm = node2comm.get(this.getNode(i))
        val dstComms = this.getEdges(i).map(node2comm.get)
        dstComms.indices.map(j => {
          val (src, dst) = srcComm -> dstComms(j)
          (src, dst) -> this.getNodeEdgePro(i, j)
        })
      }.filter(x => x._1._1 != x._1._2)
    }
  }

  def refreshQueueStatus(model: LeidenPSModel): this.type = {
    this.makeBatchIterator().foreach { case (start, end) =>
      val batchNodes = this.superNodes.slice(start, end)
      // node of status large than 0 would inserted new visit queue
      val node2status = model.getRankNodeStatus(batchNodes).getStorage
      val indexes = Iterator.range(start, end).filter(i => node2status.get(this.getNode(i)) > 0).toArray
      if (indexes.nonEmpty) indexes.foreach(x => this.queue.enqueue(x))
    }
    this
  }

  def makeBatchIterator(): Iterator[(Int, Int)] = new Iterator[(Int, Int)] {
    var index = 0

    override def next(): (Int, Int) = {
      val preIndex = index
      index = index + batchSize
      (preIndex, math.min(index, superNodes.length))
    }

    override def hasNext: Boolean = {
      index < superNodes.length
    }
  }
}

object CSR extends Serializable {
  def apply(id: Int,
            nodes: Array[Long],
            nodeWeights: Array[Float],
            edgeWeights: Array[Float],
            adj: Array[Array[Long]],
            adjWeights: Array[Array[Float]],
            batchSize: Int
           ): CSR = {
    assert(nodes.length == nodeWeights.length &&
      nodeWeights.length == edgeWeights.length &&
      edgeWeights.length == adj.length &&
      adj.length == adjWeights.length)


    val indexes = Array.fill(nodes.length + 1)(0)
    indexes.indices.foreach(i => if (i > 0) indexes(i) = indexes(i - 1) + adj(i - 1).length)

    val edges = adj.flatten[Long]
    val edgePros = adjWeights.flatten[Float]
    val nodePros = nodeWeights
    val innerEdgePros = edgeWeights

    new CSR(id, nodes, nodePros, innerEdgePros, indexes, edges, edgePros, batchSize)
  }
}

