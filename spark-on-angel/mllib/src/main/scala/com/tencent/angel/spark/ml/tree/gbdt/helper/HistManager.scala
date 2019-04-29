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


package com.tencent.angel.spark.ml.tree.gbdt.helper

import java.util.concurrent.{Callable, ExecutorService, Future}

import com.tencent.angel.spark.ml.tree.gbdt.dataset.{Dataset, Partition}
import com.tencent.angel.spark.ml.tree.gbdt.histogram.{GradPair, Histogram}
import com.tencent.angel.spark.ml.tree.gbdt.metadata.FeatureInfo
import com.tencent.angel.spark.ml.tree.gbdt.metadata.InstanceInfo
import com.tencent.angel.spark.ml.tree.param.GBDTParam
import com.tencent.angel.spark.ml.tree.util.{ConcurrentUtil, Maths}

import scala.collection.mutable.ArrayBuffer

object HistManager {

  type NodeHist = Array[Histogram]

  private val MIN_INSTANCE_PER_THREAD = 10000
  private val MAX_INSTANCE_PER_THREAD = 1000000

  // for root node
  def sparseBuild(param: GBDTParam, partition: Partition[Int, Int],
                  insIdOffset: Int, instanceInfo: InstanceInfo, start: Int, end: Int,
                  isFeatUsed: Array[Boolean], histograms: NodeHist): Unit = {
    val gradients = instanceInfo.gradients
    val hessians = instanceInfo.hessians
    val fids = partition.indices
    val bins = partition.values
    val indexEnds = partition.indexEnds
    if (param.numClass == 2 || param.isMultiClassMultiTree) {
      var indexStart = if (start == 0) 0 else indexEnds(start - 1)
      for (i <- start until end) {
        val insId = i + insIdOffset
        val grad = gradients(insId)
        val hess = hessians(insId)
        val indexEnd = indexEnds(i)
        for (j <- indexStart until indexEnd) {
          if (isFeatUsed(fids(j))) {
            histograms(fids(j)).accumulate(bins(j), grad, hess)
          }
        }
        indexStart = indexEnd
      }
    } else if (!param.fullHessian) {
      var indexStart = if (start == 0) 0 else indexEnds(start - 1)
      for (i <- start until end) {
        val insId = i + insIdOffset
        val indexEnd = indexEnds(i)
        for (j <- indexStart until indexEnd) {
          if (isFeatUsed(fids(j))) {
            histograms(fids(j)).accumulate(bins(j),
              gradients, hessians, insId * param.numClass)
          }
        }
        indexStart = indexEnd
      }
    } else {
      throw new UnsupportedOperationException("Full hessian not supported")
    }
  }

  // for nodes with few instances
  def sparseBuild(param: GBDTParam, dataset: Dataset[Int, Int],
                  instanceInfo: InstanceInfo, insIds: Array[Int], start: Int, end: Int,
                  isFeatUsed: Array[Boolean], histograms: NodeHist): Unit = {
    val gradients = instanceInfo.gradients
    val hessians = instanceInfo.hessians
    val insLayouts = dataset.insLayouts
    val partitions = dataset.partitions
    val partOffsets = dataset.partOffsets
    if (param.numClass == 2 || param.isMultiClassMultiTree) {
      for (i <- start until end) {
        val insId = insIds(i)
        val grad = gradients(insId)
        val hess = hessians(insId)
        val partId = insLayouts(insId)
        val fids = partitions(partId).indices
        val bins = partitions(partId).values
        val partInsId = insId - partOffsets(partId)
        val indexStart = if (partInsId == 0) 0 else partitions(partId).indexEnds(partInsId - 1)
        val indexEnd = partitions(partId).indexEnds(partInsId)
        for (j <- indexStart until indexEnd) {
          if (isFeatUsed(fids(j))) {
            histograms(fids(j)).accumulate(bins(j), grad, hess)
          }
        }
      }
    } else if (!param.fullHessian) {
      for (i <- start until end) {
        val insId = insIds(i)
        val partId = insLayouts(insId)
        val fids = partitions(partId).indices
        val bins = partitions(partId).values
        val partInsId = insId - partOffsets(partId)
        val indexStart = if (partInsId == 0) 0 else partitions(partId).indexEnds(partInsId - 1)
        val indexEnd = partitions(partId).indexEnds(partInsId)
        for (j <- indexStart until indexEnd) {
          if (isFeatUsed(fids(j))) {
            histograms(fids(j)).accumulate(bins(j),
              gradients, hessians, insId * param.numClass)
          }
        }
      }
    } else {
      throw new UnsupportedOperationException("Full hessian not supported")
    }
  }

  def fillDefaultBins(param: GBDTParam, featureInfo: FeatureInfo,
                      sumGradPair: GradPair, histograms: NodeHist): Unit = {
    for (fid <- histograms.indices) {
      if (histograms(fid) != null) {
        val taken = histograms(fid).sum()
        val remain = sumGradPair.subtract(taken)
        val defaultBin = featureInfo.getDefaultBin(fid)
        histograms(fid).accumulate(defaultBin, remain)
      }
    }
  }

  private def allocNodeHist(param: GBDTParam, featureInfo: FeatureInfo,
                            isFeatUsed: Array[Boolean]): NodeHist = {
    val numFeat = featureInfo.numFeature
    val histograms = Array.ofDim[Histogram](numFeat)
    for (fid <- 0 until numFeat) {
      if (isFeatUsed(fid))
        histograms(fid) = new Histogram(featureInfo.getNumBin(fid),
          param.numClass, param.fullHessian, param.isMultiClassMultiTree)
    }
    histograms
  }

  def apply(param: GBDTParam, featureInfo: FeatureInfo): HistManager = new HistManager(param, featureInfo)

}

import HistManager._

class HistManager(param: GBDTParam, featureInfo: FeatureInfo) {
  private[gbdt] var isFeatUsed: Array[Boolean] = _
  private[gbdt] val nodeGradPairs = new Array[GradPair](Maths.pow(2, param.maxDepth + 1) - 1)
  private[gbdt] var nodeHists = new Array[NodeHist](Maths.pow(2, param.maxDepth) - 1)
  private[gbdt] var histStore = new Array[NodeHist](Maths.pow(2, param.maxDepth) - 1)
  private[gbdt] var availHist = 0

  private class NodeHistPool(capacity: Int) {
    private val pool = Array.ofDim[NodeHist](capacity)
    private var numHist = 0
    private var numAcquired = 0

    private[gbdt] def acquire: NodeHist = {
      this.synchronized {
        if (numHist == numAcquired) {
          require(numHist < pool.length)
          pool(numHist) = getOrAllocSync(sync = true)
          numHist += 1
        }
        var i = 0
        while (i < numHist && pool(i) == null) i += 1
        numAcquired += 1
        val nodeHist = pool(i)
        pool(i) = null
        nodeHist
      }
    }

    private[gbdt] def release(nodeHist: NodeHist): Unit = {
      this.synchronized {
        require(numHist > 0)
        var i = 0
        while (i < numHist && pool(i) != null) i += 1
        pool(i) = nodeHist
        numAcquired -= 1
      }
    }

    private[gbdt] def result: NodeHist = {
      require(numHist > 0 && numAcquired == 0)
      val res = pool.head
      for (i <- 1 until numHist) {
        val one = pool(i)
        for (fid <- isFeatUsed.indices)
          if (res(fid) != null)
            res(fid).plusBy(one(fid))
        releaseSync(one, sync = true)
      }
      res
    }
  }

  def buildHistForRoot(dataset: Dataset[Int, Int], instanceInfo: InstanceInfo,
                       threadPool: ExecutorService = null): Unit = {
    val histograms = if (param.numThread == 1 || dataset.size < MIN_INSTANCE_PER_THREAD) {
      val nodeHist = getOrAllocSync()
      for (partId <- 0 until dataset.numPartition) {
        val partition = dataset.partitions(partId)
        val insIdOffset = dataset.partOffsets(partId)
        sparseBuild(param, partition, insIdOffset, instanceInfo,
          0, partition.size, isFeatUsed, nodeHist)
      }
      nodeHist
    } else {
      val histPool = new NodeHistPool(param.numThread)
      val futures = ArrayBuffer[Future[Unit]]()
      val batchSize = MIN_INSTANCE_PER_THREAD max (MAX_INSTANCE_PER_THREAD min
        Maths.idivCeil(dataset.size, param.numThread))
      (0 until dataset.numPartition).foreach(partId => {
        val partition = dataset.partitions(partId)
        val insIdOffset = dataset.partOffsets(partId)
        val thread = (start: Int, end: Int) => {
          val nodeHist = histPool.acquire
          nodeHist.synchronized {
            sparseBuild(param, partition, insIdOffset, instanceInfo,
              start, end, isFeatUsed, nodeHist)
          }
          histPool.release(nodeHist)
        }
        futures ++= ConcurrentUtil.rangeParallel(thread, 0, partition.size,
          threadPool, batchSize = batchSize)
      })
      futures.foreach(_.get)
      histPool.result
    }
    fillDefaultBins(param, featureInfo, nodeGradPairs(0), histograms)
    setNodeHist(0, histograms)
  }

  def buildHistForNodes(nids: Seq[Int], dataset: Dataset[Int, Int], instanceInfo: InstanceInfo,
                        subtracts: Seq[Boolean], threadPool: ExecutorService = null): Unit = {
    if (param.numThread == 1 || nids.length == 1) {
      (nids, subtracts).zipped.foreach {
        case (nid, subtract) => buildHistForNode(nid, dataset, instanceInfo, subtract)
      }
    } else {
      val futures = (nids, subtracts).zipped.map {
        case (nid, subtract) => threadPool.submit(new Callable[Unit] {
          override def call(): Unit =
            buildHistForNode(nid, dataset, instanceInfo, subtract, sync = true)
        })
      }
      futures.foreach(_.get())
    }
  }

  def buildHistForNode(nid: Int, dataset: Dataset[Int, Int], instanceInfo: InstanceInfo,
                       subtract: Boolean = false, sync: Boolean = false): Unit = {
    val nodeHist = getOrAllocSync(sync = sync)
    sparseBuild(param, dataset, instanceInfo, instanceInfo.nodeToIns,
      instanceInfo.getNodePosStart(nid), instanceInfo.getNodePosEnd(nid),
      isFeatUsed, nodeHist)
    fillDefaultBins(param, featureInfo, nodeGradPairs(nid), nodeHist)
    setNodeHist(nid, nodeHist)
    if (subtract) {
      histSubtract(nid, Maths.sibling(nid), Maths.parent(nid))
    } else {
      removeNodeHist(Maths.parent(nid), sync = sync)
    }
  }

  def getGradPair(nid: Int): GradPair = nodeGradPairs(nid)

  def setGradPair(nid: Int, gp: GradPair): Unit = nodeGradPairs(nid) = gp

  def getNodeHist(nid: Int): NodeHist = nodeHists(nid)

  def setNodeHist(nid: Int, nodeHist: NodeHist): Unit = nodeHists(nid) = nodeHist

  def removeNodeHist(nid: Int, sync: Boolean = false): Unit = {
    val nodeHist = nodeHists(nid)
    if (nodeHist != null) {
      nodeHists(nid) = null
      releaseSync(nodeHist, sync = sync)
    }
  }

  def histSubtract(nid: Int, sibling: Int, parent: Int): Unit = {
    val mined = nodeHists(parent)
    val miner = nodeHists(nid)
    for (fid <- isFeatUsed.indices)
      if (mined(fid) != null)
        mined(fid).subtractBy(miner(fid))
    nodeHists(sibling) = mined
    nodeHists(parent) = null
  }

  private def getOrAllocSync(sync: Boolean = false): NodeHist = {
    def doGetOrAlloc(): NodeHist = {
      if (availHist == 0) {
        allocNodeHist(param, featureInfo, isFeatUsed)
      } else {
        val res = histStore(availHist - 1)
        histStore(availHist - 1) = null
        availHist -= 1
        res
      }
    }

    if (sync) this.synchronized(doGetOrAlloc())
    else doGetOrAlloc()
  }

  private def releaseSync(nodeHist: NodeHist, sync: Boolean = false): Unit = {
    def doRelease(): Unit = {
      for (hist <- nodeHist)
        if (hist != null)
          hist.clear()
      histStore(availHist) = nodeHist
      availHist += 1
    }

    if (sync) this.synchronized(doRelease())
    else doRelease()
  }

  def reset(isFeatUsed: Array[Boolean]): Unit = {
    require(isFeatUsed.length == featureInfo.numFeature)
    this.isFeatUsed = isFeatUsed
    for (i <- histStore.indices)
      histStore(i) = null
    availHist = 0
    System.gc()
  }

}
