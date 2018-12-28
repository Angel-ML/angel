package com.tencent.angel.spark.ml.tree.gbdt.metadata

import com.tencent.angel.spark.ml.tree.util.Maths

object FeatureInfo {
  private val ENUM_THRESHOLD: Int = 16

  def apply(numFeature: Int, splits: Array[Array[Float]]): FeatureInfo = {
    require(splits.length == numFeature)
    val isCategorical = new Array[Boolean](numFeature)
    val numBin = new Array[Int](numFeature)
    val defaultBins = new Array[Int](numFeature)
    for (i <- 0 until numFeature) {
      if (splits(i) == null || splits(i)(0) != splits(i)(0)) { // NaN, which means this feature has no values
        splits(i) = null
        numBin(i) = 0
        defaultBins(i) = -1
      } else {
        if (splits(i).length <= ENUM_THRESHOLD) {
          isCategorical(i) = true
          numBin(i) = splits(i).length + 1
          defaultBins(i) = splits(i).length
        } else {
          isCategorical(i) = false
          numBin(i) = splits(i).length
          defaultBins(i) = Maths.indexOf(splits(i), 0.0f) // TODO: default bin for continuous feature
        }
      }
    }

    val empCnt = splits.count(_ == null)
    val numCnt = (splits, isCategorical).zipped.count(p => p._1 != null && !p._2)
    val catCnt = (splits, isCategorical).zipped.count(p => p._1 != null && p._2)
    println(s"Count: empty[$empCnt], numerical[$numCnt], categorical[$catCnt]")

    new FeatureInfo(isCategorical, numBin, splits, defaultBins)
  }

  def apply(featTypes: Array[Boolean], splits: Array[Array[Float]]): FeatureInfo = {
    require(featTypes.length == splits.length)
    val numFeature = featTypes.length
    val numBin = new Array[Int](numFeature)
    val defaultBins = new Array[Int](numFeature)
    for (i <- 0 until numFeature) {
      if (featTypes(i)) {
        numBin(i) = splits(i).length + 1
        defaultBins(i) = splits(i).length
      } else {
        numBin(i) = splits(i).length
        defaultBins(i) = Maths.indexOf(splits(i), 0.0f)  // TODO: default bin for continuous feature
      }
    }

    new FeatureInfo(featTypes, numBin, splits, defaultBins)
  }
}

case class FeatureInfo(featTypes: Array[Boolean], numBin: Array[Int],
                       splits: Array[Array[Float]], defaultBins: Array[Int]) {

  def isCategorical(fid: Int) = featTypes(fid)

  def getNumBin(fid: Int) = numBin(fid)

  def getSplits(fid: Int) = splits(fid)

  def getDefaultBin(fid: Int) = defaultBins(fid)
}
