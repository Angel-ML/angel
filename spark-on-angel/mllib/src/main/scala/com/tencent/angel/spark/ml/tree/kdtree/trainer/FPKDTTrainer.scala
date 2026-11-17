package com.tencent.angel.spark.ml.tree.kdtree.trainer

import java.util.concurrent.Executors

import com.tencent.angel.spark.ml.tree.kdtree.dataset.Dataset
import java.{util => ju}

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.spark.ml.tree.kdtree.helper.HistManager
import com.tencent.angel.spark.ml.tree.gbdt.metadata.FeatureInfo
import com.tencent.angel.spark.ml.tree.kdtree.helper.SplitFinder
import com.tencent.angel.spark.ml.tree.kdtree.metadata.InstanceInfo
import com.tencent.angel.spark.ml.tree.kdtree.psmodel.KDTreePSModel
import com.tencent.angel.spark.ml.tree.kdtree.tree.{KDTNode, KDTSplit, KDTree}
import com.tencent.angel.spark.ml.tree.param.KDTParam
import com.tencent.angel.spark.ml.tree.split.{SplitEntry, SplitPoint}
import com.tencent.angel.spark.ml.tree.util.{Maths, RangeBitSet}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.util.Random

class FPKDTTrainer(val workerId: Int, val param: KDTParam,
                   @transient private val featureInfo: FeatureInfo,
                   @transient private val trainData: Dataset[Int, Int]) extends Serializable {
  private[kdtree] val forest = ArrayBuffer[KDTree]()

  private[kdtree] val instanceInfo = InstanceInfo(param, trainData.size, featureInfo.numFeature)
  private[kdtree] val numFeatUsed = Math.round(featureInfo.numFeature * param.featSampleRatio)
  private[kdtree] val isFeatUsed = {
    if (numFeatUsed == featureInfo.numFeature)
      (0 until numFeatUsed).map(fid => featureInfo.getNumBin(fid) > 0).toArray
    else
      new Array[Boolean](numFeatUsed)
  }

  private[kdtree] val activeNodes = ArrayBuffer[Int]()

  @transient private[kdtree] val histManager = HistManager(param, featureInfo)
  @transient private[kdtree] val splitFinder = SplitFinder(param, featureInfo)
  @transient private[kdtree] val threadPool = if (param.numThread > 1)
    Executors.newFixedThreadPool(param.numThread) else null

  def timing[A](f: => A)(t: Long => Any): A = {
    val t0 = System.currentTimeMillis()
    val res = f
    t(System.currentTimeMillis() - t0)
    res
  }

  def createNewTree(): Unit = {
    // 1. create new tree
    val tree = new KDTree(param)
    this.forest += tree
    // 2. sample features
    if (numFeatUsed != featureInfo.numFeature) {
      ju.Arrays.fill(isFeatUsed, false)
      for (_ <- 0 until numFeatUsed) {
        val rand = Random.nextInt(featureInfo.numFeature)
        isFeatUsed(rand) = featureInfo.getNumBin(rand) > 0
      }
      histManager.reset(isFeatUsed)
    } else if (forest.length == 1) {
      histManager.reset(isFeatUsed)
    }
    // 3. reset position info
    instanceInfo.resetPosInfo()
    // 4. calc sumPairs
    val sumPairs = instanceInfo.calcSumPairs(featureInfo, trainData)
    tree.getRoot.setSumPairs(sumPairs)
    (0 until sumPairs.length).foreach {fid => histManager.setSumPair(0, fid, sumPairs(fid))}

    // 5. set root status
    activeNodes += 0
  }

  def findSplits(): Seq[(Int, KDTSplit)] = {
    val res = if (activeNodes.nonEmpty) {
      buildHistAndFindSplit(activeNodes)
    } else {
      Seq.empty
    }
    activeNodes.clear()
    res
  }

  def getSplitResults(splits: Seq[(Int, Int, Int, KDTSplit)]): Seq[(Int, RangeBitSet)] = {
    val tree = forest.last
    splits.flatMap {
      case (nid, ownerId, fidInWorker, split) =>
        tree.getNode(nid).setSplitEntry(split.getSplitEntry)
        if (ownerId == this.workerId) {
          histManager.setSumPair(2 * nid + 1, fidInWorker, split.getLeftSumPairs()(fidInWorker))
          histManager.setSumPair(2 * nid + 2, fidInWorker, split.getRightSumPairs()(fidInWorker))
          val res = Iterator((nid, getSplitResult(nid, fidInWorker, split.getSplitEntry, split)))
          (0 until featureInfo.numFeature).foreach{ fid =>
            if (fid != fidInWorker) {
              histManager.setSumPair(2 * nid + 1, fid, split.getLeftSumPairs()(fid))
              histManager.setSumPair(2 * nid + 2, fid, split.getRightSumPairs()(fid))
            }
          }
          res
        } else {
          Iterator.empty
        }
    }
  }

  def splitNodes(splits: Seq[(Int, Int, Int, KDTSplit)], splitResults: Seq[(Int, RangeBitSet)]): Boolean = {
    splitResults.foreach {
      case (nid, result) =>
        splitNode(nid, splits, result)
        if (2 * nid + 1 < Maths.pow(2, param.maxDepth) - 1) {
          activeNodes += 2 * nid + 1
          activeNodes += 2 * nid + 2
        } else {
          histManager.removeNodeHist(nid)
        }
    }
    activeNodes.nonEmpty
  }

  /**
    *
    * @param nids: active tree nodes
    * @return
    */
  def buildHistAndFindSplit(nids: Seq[Int]): Seq[(Int, KDTSplit)] = {

    val canSplits = nids.map(canSplitNode)

    val buildStart = System.currentTimeMillis()
    var cur = 0
    val toBuild = ArrayBuffer[Int]()
    val toSubtract = ArrayBuffer[Boolean]()
    while (cur < nids.length) {FPKDTTrainerWrapper
      val nid = nids(cur)
      val sibNid = Maths.sibling(nid)
      if (cur + 1 < nids.length && nids(cur + 1) == sibNid) {
        if (canSplits(cur) || canSplits(cur + 1)) {
          val curSize = instanceInfo.getNodeSize(nid)
          val sibSize = instanceInfo.getNodeSize(sibNid)
          if (canSplits(cur + 1)) {// curSize < sibSize
            toBuild += sibNid
            toSubtract += canSplits(cur + 1)
          } else {
            toBuild += nid
            toSubtract += canSplits(cur)
          }
        } else {
          histManager.removeNodeHist(Maths.parent(nid))
        }
        cur += 2
      } else {
        if (canSplits(cur)) {
          toBuild += nid
          toSubtract += false
        }
        cur += 1
      }
    }

    if (toBuild.nonEmpty) {
        if (toBuild.head == 0) {
          histManager.buildHistForRoot(trainData, instanceInfo, featureInfo, threadPool)
        } else {
          histManager.buildHistForNodes(toBuild, trainData, instanceInfo, toSubtract, threadPool)
        }
    }

    val findStart = System.currentTimeMillis()

    val res = (nids, canSplits).zipped.map {
      case (nid, true) =>

        val hist = histManager.getNodeHist(nid)
        val sumPair = histManager.getSumPairs(nid)
        val split =
          splitFinder.findBestSplit(hist, sumPair, 0, nid)
        (nid, if (split.isValid(param.minSplitVariance)) split else new KDTSplit())
      case (nid, false) =>
        setAsLeaf(nid)
        (nid, new KDTSplit())
    }.filter(_._2.isValid(param.minSplitVariance))
    res
  }

  def getSplitResult(nid: Int, fidInWorker: Int, splitEntry: SplitEntry, split: KDTSplit): RangeBitSet = {
    require(!splitEntry.isEmpty && splitEntry.getGain >= param.minSplitVariance)
    val splits = featureInfo.getSplits(fidInWorker)
    instanceInfo.getSplitResult(nid, fidInWorker, splitEntry, splits, trainData, split, threadPool)
  }

  def splitNode(nid: Int, splits: Seq[(Int, Int, Int, KDTSplit)], splitResult: RangeBitSet): Unit = {
      instanceInfo.updatePos(nid, splitResult)
      splits.foreach {
        case (nid, ownerId, _, split) =>

          if (ownerId != this.workerId) {
            val leftSumPairs = instanceInfo.splitResult(2 * nid + 1, trainData, featureInfo.numFeature)
            val rightSumPairs = instanceInfo.splitResult(2 * nid + 2, trainData, featureInfo.numFeature)
            histManager.setSumPairs(2 * nid + 1, leftSumPairs)
            histManager.setSumPairs(2 * nid + 2, rightSumPairs)
          }
      }
      val tree = forest.last
      val node = tree.getNode(nid)
      val leftChild = new KDTNode(2 * nid + 1, node)
      val rightChild = new KDTNode(2 * nid + 2, node)
      node.setLeftChild(leftChild)
      node.setRightChild(rightChild)
      tree.setNode(2 * nid + 1, leftChild)
      tree.setNode(2 * nid + 2, rightChild)

      leftChild.setSumPairs(histManager.getSumPairs(2 * nid + 1))
      rightChild.setSumPairs(histManager.getSumPairs(2 * nid + 2))
  }

  def canSplitNode(nid: Int): Boolean = {
    if (instanceInfo.getNodeSize(nid) > param.minNodeInstance) {
      val sumPairs = histManager.getSumPairs(nid)
      for (fid <- 0 until sumPairs.length) {
        val sumPair = sumPairs(fid)
        if (sumPair != null) {
          if (sumPair.getCnt() > param.minNodeInstance) return true
        }

      }
      false
    } else {
      false
    }
  }

  def setAsLeaf(nid: Int): Unit = setAsLeaf(nid, forest.last.getNode(nid))

  def setAsLeaf(nid: Int, node: KDTNode): Unit = {
    node.chgToLeaf()
    // TODO: update path of all training instance together
    var path = " nid: " + nid
    var kdNode = node
    while(kdNode.getParent != null) {
      val parent = kdNode.getParent.asInstanceOf[KDTNode]
      val flag = if (kdNode.equals(parent.getLeftChild)) " <= " else " > "
      path = " nid[" + parent.getNid + "] fid[" + parent.getSplitEntry.getFid + "]" +
        "" + flag + parent.getSplitEntry.asInstanceOf[SplitPoint].getFvalue + "; " + path
      kdNode = parent
    }

    if (nid < Maths.pow(2, param.maxDepth) - 1) // node not on the last level
      histManager.removeNodeHist(nid)
  }

  def finishTree(): Unit = {
    forest.last.getNodes.asScala.foreach {
      case (nid, node) =>
        if (node.getSplitEntry == null && !node.isLeaf)
          setAsLeaf(nid, node)
    }
  }

  def getInstanceNum(): Int = {
    trainData.numInstance
  }

  def getTree(): Seq[KDTree] = {
    println(s"Worker[$workerId] get tree ...")
    if (threadPool != null) threadPool.shutdown()

    forest
  }

  def getInstanceInfo(): InstanceInfo = {
    println(s"Worker[$workerId] get info ...")
    if (threadPool != null) threadPool.shutdown()

    instanceInfo
  }

  def getTreeData(model: KDTreePSModel, bcGroupIdToFid: Array[Int], featureNum: Int, batchSize: Int): Unit = {
    val start0 = instanceInfo.nodePosStart(0)
    val end0 = instanceInfo.nodePosEnd(0)
    var epoch = (end0 - start0 + 1) / batchSize
    if ((end0 - start0 + 1) % batchSize != 0) epoch += 1

    (0 until epoch).flatMap{ i =>
      val size = if ((i + 1) * batchSize <= (end0 - start0 + 1)) batchSize else (end0 - start0 + 1) % batchSize
      val insIds = new ArrayBuffer[Long]()
      val ferats = (i * batchSize until i * batchSize + size).flatMap{ posId =>
        val insId = instanceInfo.nodeToIns(posId)
//        val insLabel = labels(insId)
        insIds.append(insId.toLong)
        val indices = new Array[Int](featureInfo.numFeature)
        val values = new Array[Float](featureInfo.numFeature)
        (0 until featureInfo.numFeature).foreach{ fid =>
          indices(fid) = bcGroupIdToFid(fid)
          values(fid) = trainData.getFvalue(insId, fid)
        }
        Iterator(VFactory.sortedFloatVector(featureNum, indices, values))
      }.toArray
      Iterator((insIds.toArray, ferats))
    }.foreach{ batch =>
      if (workerId == 0) {
        model.initNodeFeatures(batch._1, batch._2)
      } else {
        model.updateFeatures(batch._1, batch._2)
      }

    }
  }


}
