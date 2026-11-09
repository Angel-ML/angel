package com.tencent.angel.spark.ml.tree.kdtree.metadata

import java.util.concurrent.ExecutorService

import com.tencent.angel.spark.ml.tree.gbdt.metadata.FeatureInfo
import com.tencent.angel.spark.ml.tree.kdtree.histogram.SumPair
import com.tencent.angel.spark.ml.tree.kdtree.dataset.Dataset
import com.tencent.angel.spark.ml.tree.kdtree.tree.KDTSplit
import com.tencent.angel.spark.ml.tree.param.KDTParam
import com.tencent.angel.spark.ml.tree.split.{SplitEntry, SplitPoint}
import com.tencent.angel.spark.ml.tree.util.{ConcurrentUtil, Maths, RangeBitSet}

case class InstanceInfo(param: KDTParam,
                   nodePosStart: Array[Int], nodePosEnd: Array[Int], nodeToIns: Array[Int]) {


  def resetPosInfo(): Unit = {
    val num = nodeToIns.length
    nodePosStart(0) = 0
    nodePosEnd(0) = num - 1
    for (i <- 0 until num) {
      nodeToIns(i) = i
    }
  }

  def calcSumPairs(featureInfo: FeatureInfo, data: Dataset[Int, Int], threadPool: ExecutorService = null): Array[SumPair] = {

    def calcV(start: Int, end: Int): Array[SumPair] = {
      val featureNum = featureInfo.numFeature

      val sumPairs = (0 until featureNum).map { fid =>
        var sumValue = 0.0f
        var sumSquare = 0.0f

        for (insId <- start until end) {
          val value = data.getFvalue(insId, fid)
          val square = value * value
          sumValue += value
          sumSquare += square
        }

        new SumPair(end - start)
      }
      sumPairs.toArray
    }

    val numIns = data.size
    calcV(0, numIns)
  }

  def getSplitResult(nid: Int, fidInWorker: Int, splitEntry: SplitEntry, splits: Array[Float],
                     dataset: Dataset[Int, Int], kdSplit: KDTSplit, threadPool: ExecutorService = null): RangeBitSet = {
    val res = new RangeBitSet(nodePosStart(nid), nodePosEnd(nid))

    def split(start: Int, end: Int): Unit = {

      for (posId <- start until end) {
        val insId = nodeToIns(posId)
        val v = dataset.getFvalue(insId, fidInWorker)

        val flowTo = if (v >= 0) {
          splitEntry.asInstanceOf[SplitPoint].flowToLessOrEqual(v)
        } else {
          splitEntry.defaultTo()
        }

        if (flowTo == 1) {
          res.set(posId)
          (0 until kdSplit.getLeftSumPairs.length).foreach{ fid =>
            if (fidInWorker != fid && kdSplit.getLeftSumPairs != null && kdSplit.getLeftSumPairs()(fid) != null) {
              kdSplit.setLeftSumPair(fid, kdSplit.getLeftSumPairs()(fid).subtract(1))
            }
          }

        } else {
          (0 until kdSplit.getLeftSumPairs.length).foreach{ fid =>
            if (fidInWorker != fid && kdSplit.getRightSumPairs != null && kdSplit.getRightSumPairs()(fid) != null) {
              kdSplit.setRightSumPair(fid, kdSplit.getRightSumPairs()(fid).subtract(1))
            }
          }
        }
      }
    }

    if (threadPool == null) {
      split(nodePosStart(nid), nodePosEnd(nid) + 1)
    } else {
      ConcurrentUtil.rangeParallel(split, nodePosStart(nid), nodePosEnd(nid) + 1, threadPool)
        .foreach(_.get())
    }
    res
  }


  def splitResult(nid: Int,
                  dataset: Dataset[Int, Int], featureNum: Int, threadPool: ExecutorService = null): Array[SumPair] = {

    def split(start: Int, end: Int): Array[SumPair] = {
      val sumPairs = new Array[SumPair](featureNum)
      (0 until featureNum).foreach(i => sumPairs(i) = new SumPair())

      for (posId <- start until end) {

        (0 until featureNum).foreach{ fid =>
          sumPairs(fid).plusBy(1)
        }
      }
      sumPairs
    }

    split(nodePosStart(nid), nodePosEnd(nid) + 1)//todo???
    if (threadPool == null) {
      split(nodePosStart(nid), nodePosEnd(nid) + 1)
    } else {
      ConcurrentUtil.rangeParallel(split, nodePosStart(nid), nodePosEnd(nid) + 1, threadPool)
        .flatMap(_.get())
    }

  }


  def updatePos(nid: Int, splitResult: RangeBitSet): Unit = {
    val nodeStart = nodePosStart(nid)
    val nodeEnd = nodePosEnd(nid)
    var left = nodeStart
    var right = nodeEnd
    while (left < right) {
      while (left < right && !splitResult.get(left)) left += 1
      while (left < right && splitResult.get(right)) right -= 1
      if (left < right) {
        val leftInsId = nodeToIns(left)
        val rightInsId = nodeToIns(right)
        nodeToIns(left) = rightInsId
        nodeToIns(right) = leftInsId
        left += 1
        right -= 1
      }
    }
    // find the cut position
    val cutPos = if (left == right) {
      if (splitResult.get(left)) left - 1
      else left
    } else {
      right
    }
    nodePosStart(2 * nid + 1) = nodeStart
    nodePosEnd(2 * nid + 1) = cutPos
    nodePosStart(2 * nid + 2) = cutPos + 1
    nodePosEnd(2 * nid + 2) = nodeEnd
  }

  def numInstance: Int = nodeToIns.length

  def getNodePosStart(nid: Int) = nodePosStart(nid)

  def getNodePosEnd(nid: Int) = nodePosEnd(nid)

  def getNodeSize(nid: Int): Int = nodePosEnd(nid) - nodePosStart(nid) + 1
}


object InstanceInfo {
  def apply(param: KDTParam, numData:Int, numFeatures: Int): InstanceInfo = {

    val maxNodeNum = Maths.pow(2, param.maxDepth + 1) - 1
    val nodePosStart = new Array[Int](maxNodeNum)
    val nodePosEnd = new Array[Int](maxNodeNum)
    val nodeToIns = new Array[Int](numData)
    InstanceInfo(param, nodePosStart, nodePosEnd, nodeToIns)
  }
}