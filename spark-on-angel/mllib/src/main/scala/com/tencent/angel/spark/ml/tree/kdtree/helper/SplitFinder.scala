package com.tencent.angel.spark.ml.tree.kdtree.helper

import com.tencent.angel.spark.ml.tree.gbdt.metadata.FeatureInfo
import com.tencent.angel.spark.ml.tree.kdtree.tree.KDTSplit
import com.tencent.angel.spark.ml.tree.param.KDTParam
import SplitFinder._
import com.tencent.angel.spark.ml.tree.kdtree.histogram.{Histogram, SumPair}
import com.tencent.angel.spark.ml.tree.split.SplitPoint

class SplitFinder(param: KDTParam, featureInfo: FeatureInfo) {

  def findBestSplit(histograms: Array[Histogram], sumPairs: Array[SumPair],
                    nodeGain: Float, nid: Int): KDTSplit = {
    val best = new KDTSplit()
    for (fid <- histograms.indices) {
      if (histograms(fid) != null) {
        val cur = findBestSplitPoint(param, nid, fid, featureInfo.getSplits(fid), featureInfo.getCounts(fid),
          histograms(fid), sumPairs)
        best.update(cur)
      }
    }
    for (fid <- histograms.indices) {
      if (histograms(fid) != null && best.getSplitEntry != null) {
        if (best.getSplitEntry.getFid == fid) {
          histograms(fid).setSplitFlag(true)
        } else {
          histograms(fid).setSplitFlag(false)
          best.setLeftSumPair(fid, sumPairs(fid))
          best.setRightSumPair(fid, sumPairs(fid))
        }
      }
    }
    best
  }
}

object SplitFinder {

  def findBestSplitPoint(param: KDTParam, nid: Int, fid: Int, splits: Array[Float], counts: Array[Int],
                         histogram: Histogram, sumPairs: Array[SumPair]): KDTSplit = {

    if (sumPairs(fid) == null) {
      return new KDTSplit()
    }
    val splitPoint = new SplitPoint()
    val leftStat = new SumPair()
    val rightStat = sumPairs(fid).copy()
    val bestLeftStat = new Array[SumPair](sumPairs.length)
    val bestRightStat = new Array[SumPair](sumPairs.length)
    val middleBin = if (histogram.getNumBin > 1) {
      var count = 0
      var j = histogram.getStartBinIdx
      if (histogram.getStartBinIdx == 0 && histogram.getEndBinIdx >= histogram.getStartBinIdx && histogram.getEndBinIdx<counts.length) {
        count = counts(histogram.getEndBinIdx)
        while(j < histogram.getEndBinIdx && counts(j) < count / 2) j += 1
        j
      } else {
        if (histogram.getEndBinIdx >= histogram.getStartBinIdx && histogram.getEndBinIdx<counts.length && histogram.getStartBinIdx > 0) {
          count = counts(histogram.getEndBinIdx) - counts(histogram.getStartBinIdx - 1)
          while(j < histogram.getEndBinIdx && counts(j) - counts(histogram.getStartBinIdx - 1) < count / 2)
            j += 1
          j
        } else histogram.getStartBinIdx

      }
    } else 0

    if(middleBin >= 0 && histogram.getNumBin != 0 && middleBin < splits.length) {
      histogram.scan(middleBin, leftStat, rightStat)
      val balanced = math.abs(leftStat.getCnt - rightStat.getCnt)
      if (splitPoint.needReplace(balanced)) {
        splitPoint.setFid(fid)
        splitPoint.setFvalue(splits(middleBin))
        splitPoint.setGain(balanced)
        bestLeftStat(fid) = leftStat.copy()
        bestRightStat(fid) = rightStat.copy()
      }
    }
    new KDTSplit(splitPoint, bestLeftStat, bestRightStat)
  }

  def apply(param: KDTParam, featureInfo: FeatureInfo): SplitFinder = new SplitFinder(param, featureInfo)
}
