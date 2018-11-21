package com.tencent.angel.ml.tree.data

import com.tencent.angel.ml.tree.impurity._

/**
  * DecisionTree statistics aggregator for a node.
  * This holds a flat array of statistics for a set of (features, bins)
  * and helps with indexing.
  * This class is abstract to support learning with and without feature subsampling.
  */
private[tree] class DTStatsAggregator(
                                        val metadata: DecisionTreeMetadata,
                                        featureSubset: Option[Array[Int]]) extends Serializable {

  /**
    * [[ImpurityAggregator]] instance specifying the impurity type.
    */
  val impurityAggregator: ImpurityAggregator = metadata.impurity match {
    case Gini => new GiniAggregator(metadata.numClasses)
    case Entropy => new EntropyAggregator(metadata.numClasses)
    case Variance => new VarianceAggregator()
    case _ => throw new IllegalArgumentException(s"Bad impurity parameter: ${metadata.impurity}")
  }

  /**
    * Number of elements (Double values) used for the sufficient statistics of each bin.
    */
  private val statsSize: Int = impurityAggregator.statsSize

  /**
    * Number of bins for each feature.  This is indexed by the feature index.
    */
  private val numBins: Array[Int] = {
    if (featureSubset.isDefined) {
      featureSubset.get.map(metadata.numBins(_))
    } else {
      metadata.numBins
    }
  }

  /**
    * Offset for each feature for calculating indices into the [[allStats]] array.
    */
  private val featureOffsets: Array[Int] = {
    numBins.scanLeft(0)((total, nBins) => total + statsSize * nBins)
  }

  /**
    * Total number of elements stored in this aggregator
    */
  private val allStatsSize: Int = featureOffsets.last

  /**
    * Flat array of elements.
    * Index for start of stats for a (feature, bin) is:
    *   index = featureOffsets(featureIndex) + binIndex * statsSize
    */
  private val allStats: Array[Float] = new Array[Float](allStatsSize)

  /**
    * Array of parent node sufficient stats.
    * Note: parent stats need to be explicitly tracked in the [[DTStatsAggregator]] for unordered
    *       categorical features, because the parent [[Node]] object does not have [[ImpurityStats]]
    *       on the first iteration.
    */
  private val parentStats: Array[Float] = new Array[Float](statsSize)

  /**
    * Get an [[ImpurityCalculator]] for a given (node, feature, bin).
    *
    * @param featureOffset  This is a pre-computed (node, feature) offset
    *                           from [[getFeatureOffset]].
    */
  def getImpurityCalculator(featureOffset: Int, binIndex: Int): ImpurityCalculator = {
    impurityAggregator.getCalculator(allStats, featureOffset + binIndex * statsSize)
  }

  /**
    * Get an [[ImpurityCalculator]] for the parent node.
    */
  def getParentImpurityCalculator(): ImpurityCalculator = {
    impurityAggregator.getCalculator(parentStats, 0)
  }

  /**
    * Update the stats for a given (feature, bin) for ordered features, using the given label.
    */
  def update(featureIndex: Int, binIndex: Int, label: Float, instanceWeight: Float): Unit = {
    val i = featureOffsets(featureIndex) + binIndex * statsSize
    impurityAggregator.update(allStats, i, label, instanceWeight)
  }

  /**
    * Update the parent node stats using the given label.
    */
  def updateParent(label: Float, instanceWeight: Float): Unit = {
    impurityAggregator.update(parentStats, 0, label, instanceWeight)
  }

  /**
    * Faster version of [[update]].
    * Update the stats for a given (feature, bin), using the given label.
    *
    * @param featureOffset  This is a pre-computed feature offset
    *                           from [[getFeatureOffset]].
    */
  def featureUpdate(
                     featureOffset: Int,
                     binIndex: Int,
                     label: Float,
                     instanceWeight: Float): Unit = {
    impurityAggregator.update(allStats, featureOffset + binIndex * statsSize,
      label, instanceWeight)
  }

  /**
    * Pre-compute feature offset for use with [[featureUpdate]].
    * For ordered features only.
    */
  def getFeatureOffset(featureIndex: Int): Int = featureOffsets(featureIndex)

  /**
    * For a given feature, merge the stats for two bins.
    *
    * @param featureOffset  This is a pre-computed feature offset
    *                           from [[getFeatureOffset]].
    * @param binIndex  The other bin is merged into this bin.
    * @param otherBinIndex  This bin is not modified.
    */
  def mergeForFeature(featureOffset: Int, binIndex: Int, otherBinIndex: Int): Unit = {
    impurityAggregator.merge(allStats, featureOffset + binIndex * statsSize,
      featureOffset + otherBinIndex * statsSize)
  }

  /**
    * Merge this aggregator with another, and returns this aggregator.
    * This method modifies this aggregator in-place.
    */
  def merge(other: DTStatsAggregator): DTStatsAggregator = {
    require(allStatsSize == other.allStatsSize,
      s"DTStatsAggregator.merge requires that both aggregators have the same length stats vectors."
        + s" This aggregator is of length $allStatsSize, but the other is ${other.allStatsSize}.")
    var i = 0
    while (i < allStatsSize) {
      allStats(i) += other.allStats(i)
      i += 1
    }

    require(statsSize == other.statsSize,
      s"DTStatsAggregator.merge requires that both aggregators have the same length parent " +
        s"stats vectors. This aggregator's parent stats are length $statsSize, " +
        s"but the other is ${other.statsSize}.")
    var j = 0
    while (j < statsSize) {
      parentStats(j) += other.parentStats(j)
      j += 1
    }

    this
  }
}

