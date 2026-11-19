package com.tencent.angel.spark.ml.tree.kdtree.helper

import java.util.concurrent.{Callable, ExecutorService, Future}

import com.tencent.angel.spark.ml.tree.kdtree.dataset.{Dataset, Partition}
import com.tencent.angel.spark.ml.tree.kdtree.histogram.{Histogram, SumPair}
import com.tencent.angel.spark.ml.tree.param.KDTParam
import com.tencent.angel.spark.ml.tree.util.{ConcurrentUtil, Maths}
import HistManager._
import com.tencent.angel.spark.ml.tree.gbdt.metadata.FeatureInfo
import com.tencent.angel.spark.ml.tree.kdtree.metadata.InstanceInfo

import scala.collection.mutable.ArrayBuffer

object HistManager {
  type NodeHist = Array[Histogram]
  type NodePair = Array[SumPair]

  private val MIN_INSTANCE_PER_THREAD = 10000
  private val MAX_INSTANCE_PER_THREAD = 1000000

  // for root node
  def sparseBuild(param: KDTParam, partition: Partition[Int, Int],
                  insIdOffset: Int, instanceInfo: InstanceInfo, featureInfo: FeatureInfo,
                  start: Int, end: Int, isFeatUsed: Array[Boolean], histograms: NodeHist): Unit = {

    val fids = partition.indices
    val bins = partition.values
    val indexEnds = partition.indexEnds

    var indexStart = if (start == 0) 0 else indexEnds(start - 1)

    for (i <- start until end) {

      val indexEnd = indexEnds(i)

      var tmpFid = 0
      for (j <- indexStart until indexEnd) {
        if (tmpFid <= fids(j))
          (tmpFid until fids(j)). foreach { fid =>
            if (isFeatUsed(fid)) {
              val splits = featureInfo.getSplits(fid)
              val zeroIndex = splits.indexWhere(x => x >= 0)
              histograms(fid).accumulate(zeroIndex, 1)
            }
          }
        tmpFid = fids(j) + 1
        if (isFeatUsed(fids(j))) {
          histograms(fids(j)).accumulate(bins(j), 1)
        }
      }

      if (tmpFid < featureInfo.numFeature)
        (tmpFid until featureInfo.numFeature). foreach { fid =>
          if (isFeatUsed(fid)) {
            val splits = featureInfo.getSplits(fid)
            val zeroIndex = splits.indexWhere(x => x >= 0)
            histograms(fid).accumulate(zeroIndex, 1)
          }
        }

      indexStart = indexEnd
    }

  }

  // for nodes with few instances
  def sparseBuild(param: KDTParam, dataset: Dataset[Int, Int],
                  instanceInfo: InstanceInfo, featureInfo: FeatureInfo, insIds: Array[Int],
                  start: Int, end: Int, isFeatUsed: Array[Boolean], histograms: NodeHist): Unit = {
    val insLayouts = dataset.insLayouts
    val partitions = dataset.partitions
    val partOffsets = dataset.partOffsets

    for (i <- start until end + 1) {
      val insId = insIds(i)

      val partId = insLayouts(insId)
      val fids = partitions(partId).indices
      val bins = partitions(partId).values
      val partInsId = insId - partOffsets(partId)
      val indexStart = if (partInsId == 0) 0 else partitions(partId).indexEnds(partInsId - 1)
      val indexEnd = partitions(partId).indexEnds(partInsId)
      for (j <- indexStart until indexEnd) {
        if (isFeatUsed(fids(j))) {
          histograms(fids(j)).accumulate(bins(j), 1)
        }
      }
    }

  }

//  def fillDefaultBins(param: KDTParam, featureInfo: FeatureInfo,
//                      sumPair: Array[SumPair], histograms: NodeHist): Unit = {
//    for (fid <- histograms.indices) {
//      if (histograms(fid) != null) {
//        val taken = histograms(fid).sum()
//        val remain = sumPair(fid).subtract(taken)
//        val defaultBin = featureInfo.getDefaultBin(fid)
//        histograms(fid).accumulate(defaultBin, remain)
//      }
//    }
//  }

  private def allocNodeHist(param: KDTParam, featureInfo: FeatureInfo,
                            isFeatUsed: Array[Boolean]): NodeHist = {
    val numFeat = featureInfo.numFeature
    val histograms = Array.ofDim[Histogram](numFeat)
    for (fid <- 0 until numFeat) {
      if (isFeatUsed(fid))
        histograms(fid) = new Histogram(featureInfo.getNumBin(fid))
    }
    histograms
  }

  def apply(param: KDTParam, featureInfo: FeatureInfo): HistManager = new HistManager(param, featureInfo)
}

class HistManager(param: KDTParam, featureInfo: FeatureInfo) {
  private[kdtree] var isFeatUsed: Array[Boolean] = _
  private[kdtree] val nodeSumPairs = new Array[NodePair](Maths.pow(2, param.maxDepth + 1) - 1)
  (0 until nodeSumPairs.length).foreach{ i =>
    nodeSumPairs(i) = new NodePair(featureInfo.numFeature)
  }
  private[kdtree] var nodeHists = new Array[NodeHist](Maths.pow(2, param.maxDepth + 1) - 1)
  private[kdtree] var histStore = new Array[NodeHist](Maths.pow(2, param.maxDepth + 1) - 1)
  private[kdtree] var availHist = 0

  private class NodeHistPool(capacity: Int) {
    private val pool = Array.ofDim[NodeHist](capacity)
    private var numHist = 0
    private var numAcquired = 0

    private[kdtree] def acquire: NodeHist = {
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

    private[kdtree] def release(nodeHist: NodeHist): Unit = {
      this.synchronized {
        require(numHist > 0)
        var i = 0
        while (i < numHist && pool(i) != null) i += 1
        pool(i) = nodeHist
        numAcquired -= 1
      }
    }

    private[kdtree] def result: NodeHist = {
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

  def buildHistForRoot(dataset: Dataset[Int, Int], instanceInfo: InstanceInfo, featureInfo: FeatureInfo,
                       threadPool: ExecutorService = null): Unit = {
    val histograms = {
      val nodeHist = getOrAllocSync()
      for (partId <- 0 until dataset.numPartition) {
        val partition = dataset.partitions(partId)
        val insIdOffset = dataset.partOffsets(partId)
        sparseBuild(param, partition, insIdOffset, instanceInfo, featureInfo,
          0, partition.size, isFeatUsed, nodeHist)
      }
      nodeHist
    }

    for(i <- 0 until histograms.length) {
      if (isFeatUsed(i)) {
        histograms(i).accumulateByBin(0, null.asInstanceOf[Histogram])
      }
    }

    setNodeHist(0, histograms)
  }

  def buildHistForNodes(nids: Seq[Int], dataset: Dataset[Int, Int], instanceInfo: InstanceInfo,
                        subtracts: Seq[Boolean], threadPool: ExecutorService = null): Unit = {
    (nids, subtracts).zipped.foreach {
      case (nid, subtract) => {
        buildHistForNode(nid, dataset, instanceInfo, subtract)
      }
    }
  }

  def buildHistForNode(nid: Int, dataset: Dataset[Int, Int], instanceInfo: InstanceInfo,
                       subtract: Boolean = false, sync: Boolean = false): Unit = {
    val nodeHist = getOrAllocSync(sync = sync)
    sparseBuild(param, dataset, instanceInfo, featureInfo, instanceInfo.nodeToIns,
      instanceInfo.getNodePosStart(nid), instanceInfo.getNodePosEnd(nid),
      isFeatUsed, nodeHist)
    val parent = getNodeHist(Maths.parent(nid))
//    fillDefaultBins(param, featureInfo, nodeSumPairs(nid), nodeHist)

    if (subtract && parent != null) {
      for(i <- 0 until nodeHist.length) {
        if (isFeatUsed(i)) {
          nodeHist(i).accumulateByBin(nid, parent(i))
        }
      }
      setNodeHist(nid, nodeHist)
      histSubtract(nid, Maths.sibling(nid), Maths.parent(nid))
    } else {
      setNodeHist(nid, nodeHist)
      removeNodeHist(Maths.parent(nid), sync = sync)
    }
  }

  def getSumPair(nid: Int, fid: Int): SumPair = nodeSumPairs(nid)(fid)

  def setSumPair(nid: Int, fid: Int, gp: SumPair): Unit = {
    nodeSumPairs(nid)(fid) = gp
  }

  def getSumPairs(nid: Int): NodePair = nodeSumPairs(nid)

  def setSumPairs(nid: Int, gp: NodePair): Unit = nodeSumPairs(nid) = gp

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
        val res = histStore(0)
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
