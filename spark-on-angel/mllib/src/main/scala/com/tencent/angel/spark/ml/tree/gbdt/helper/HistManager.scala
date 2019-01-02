package com.tencent.angel.spark.ml.tree.gbdt.helper

import java.util.concurrent.{Callable, Executors}

import com.tencent.angel.spark.ml.tree.gbdt.dataset.{Dataset, Partition}
import com.tencent.angel.spark.ml.tree.gbdt.histogram.{GradPair, Histogram}
import com.tencent.angel.spark.ml.tree.gbdt.metadata.FeatureInfo
import com.tencent.angel.spark.ml.tree.gbdt.metadata.org.dma.gbdt4spark.algo.gbdt.metadata.InstanceInfo
import com.tencent.angel.spark.ml.tree.tree.param.GBDTParam
import com.tencent.angel.spark.ml.tree.util.Maths

object HistManager {

  type NodeHist = Array[Histogram]

  private val MIN_INSTANCE_PER_THREAD = 100000

  // for root node
  def sparseBuild(param: GBDTParam, partition: Partition[Int, Int],
                  insIdOffset: Int, instanceInfo: InstanceInfo,
                  isFeatUsed: Array[Boolean], histograms: NodeHist): Unit = {
    val num = partition.size
    val gradients = instanceInfo.gradients
    val hessians = instanceInfo.hessians
    val fids = partition.indices
    val bins = partition.values
    val indexEnds = partition.indexEnds
    if (param.numClass == 2) {
      var indexStart = 0
      for (i <- 0 until num) {
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
      var indexStart = 0
      for (i <- 0 until num) {
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
    if (param.numClass == 2) {
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
                            isFeatUsed : Array[Boolean]): NodeHist = {
    val numFeat = featureInfo.numFeature
    val histograms = Array.ofDim[Histogram](numFeat)
    for (fid <- 0 until numFeat) {
      if (isFeatUsed(fid))
        histograms(fid) = new Histogram(featureInfo.getNumBin(fid),
          param.numClass, param.fullHessian)
    }
    histograms
  }

  def apply(param: GBDTParam, featureInfo: FeatureInfo): HistManager = new HistManager(param, featureInfo)

}

import com.tencent.angel.spark.ml.tree.gbdt.helper.HistManager._

class HistManager(param: GBDTParam, featureInfo: FeatureInfo) {
  private[gbdt] var isFeatUsed : Array[Boolean] = _
  private[gbdt] val nodeGradPairs = new Array[GradPair](Maths.pow(2, param.maxDepth + 1) - 1)
  private[gbdt] var nodeHists = new Array[NodeHist](Maths.pow(2, param.maxDepth) - 1)
  private[gbdt] var histStore = new Array[NodeHist](Maths.pow(2, param.maxDepth) - 1)
  private[gbdt] var availHist = 0

  private val threadPool = Executors.newFixedThreadPool(param.numThread)

  def buildHistForRoot(dataset: Dataset[Int, Int], instanceInfo: InstanceInfo): Unit = {
    val histograms = newNodeHist(0)
    if (param.numThread == 1 || dataset.size < MIN_INSTANCE_PER_THREAD) {
      for (partId <- 0 until dataset.numPartition) {
        val partition = dataset.partitions(partId)
        val insIdOffset = dataset.partOffsets(partId)
        sparseBuild(param, partition, insIdOffset, instanceInfo, isFeatUsed, histograms)
      }
    } else {
      val futures = (0 until dataset.numPartition).map(partId => {
        val partition = dataset.partitions(partId)
        val insIdOffset = dataset.partOffsets(partId)
        threadPool.submit(new Runnable {
          override def run(): Unit = sparseBuild(param, partition,
            insIdOffset, instanceInfo, isFeatUsed, histograms)
        })
      })
      futures.foreach(_.get)
    }
    fillDefaultBins(param, featureInfo, nodeGradPairs(0), histograms)
  }

  def buildHistForNodes(nids: Seq[Int], dataset: Dataset[Int, Int], instanceInfo: InstanceInfo,
                        subtracts: Seq[Boolean]): Unit = {
    if (param.numThread == 1 || nids.length == 1) {
      (nids, subtracts).zipped.foreach {
        case (nid, subtract) => buildHistForNode(nid, dataset, instanceInfo, subtract)
      }
    } else {
      val futures = (nids, subtracts).zipped.map {
        case (nid, subtract) => threadPool.submit(
          new Callable[Unit] {
            override def call(): Unit = buildHistForNode(nid, dataset, instanceInfo, subtract, sync = true)
          }
        )
      }
      futures.foreach(_.get())
    }
  }

  def buildHistForNode(nid: Int, dataset: Dataset[Int, Int], instanceInfo: InstanceInfo,
                       subtract: Boolean = false, sync: Boolean = false): Unit = {
    val histograms = newNodeHist(nid, sync = sync)
    sparseBuild(param, dataset, instanceInfo, instanceInfo.nodeToIns,
      instanceInfo.getNodePosStart(nid), instanceInfo.getNodePosEnd(nid),
      isFeatUsed, histograms)
    fillDefaultBins(param, featureInfo, nodeGradPairs(nid), histograms)
    if (subtract) {
      histSubtract(nid, Maths.sibling(nid), Maths.parent(nid))
    } else {
      removeNodeHist(Maths.parent(nid), sync = sync)
    }
  }

  def getGradPair(nid: Int): GradPair = nodeGradPairs(nid)

  def setGradPair(nid: Int, gp: GradPair): Unit = nodeGradPairs(nid) = gp

  def getNodeHist(nid: Int): NodeHist = nodeHists(nid)

  def newNodeHist(nid: Int, sync: Boolean = false): NodeHist = {
    val hist = if (sync)
      this.synchronized(getOrAlloc)
    else
      getOrAlloc
    nodeHists(nid) = hist
    hist
  }

  def removeNodeHist(nid: Int, sync: Boolean = false): Unit  = {
    if (nodeHists(nid) != null) {
      if (sync)
        this.synchronized(release(nid))
      else
        release(nid)
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

  private def getOrAlloc: NodeHist = {
    if (availHist == 0) {
      allocNodeHist(param, featureInfo, isFeatUsed)
    } else {
      val res = histStore(availHist - 1)
      histStore(availHist - 1) = null
      availHist -= 1
      res
    }
  }

  private def release(nid: Int): Unit = {
    val nodeHist = nodeHists(nid)
    nodeHists(nid) = null
    for (hist <- nodeHist)
      if (hist != null)
        hist.clear()
    histStore(availHist) = nodeHist
    availHist += 1
  }

  def reset(isFeatUsed: Array[Boolean]): Unit = {
    require(isFeatUsed.length == featureInfo.numFeature)
    this.isFeatUsed = isFeatUsed
    for (i <- histStore.indices)
      histStore(i) = null
    availHist = 0
    System.gc()
  }

  def shutdown(): Unit = threadPool.shutdown()

}
