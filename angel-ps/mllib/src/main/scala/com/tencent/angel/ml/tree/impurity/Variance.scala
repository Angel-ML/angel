package com.tencent.angel.ml.tree.impurity

/**
  * Class for calculating variance during regression
  */
object Variance extends Impurity {

  /**
    * information calculation for multiclass classification
    * @param counts Array[Double] with counts for each label
    * @param totalCount sum of counts for all labels
    * @return information value, or 0 if totalCount = 0
    */
  override def calculate(counts: Array[Float], totalCount: Float): Float =
    throw new UnsupportedOperationException("Variance.calculate")

  /**
    * variance calculation
    * @param count number of instances
    * @param sum sum of labels
    * @param sumSquares summation of squares of the labels
    * @return information value, or 0 if count = 0
    */
  override def calculate(count: Float, sum: Float, sumSquares: Float): Float = {
    if (count == 0) {
      return 0
    }
    val squaredLoss = sumSquares - (sum * sum) / count
    squaredLoss / count
  }

  /**
    * Get this impurity instance.
    * This is useful for passing impurity parameters to a Strategy in Java.
    */
  def instance: this.type = this

}

/**
  * Class for updating views of a vector of sufficient statistics,
  * in order to compute impurity from a sample.
  * Note: Instances of this class do not hold the data; they operate on views of the data.
  */
private class VarianceAggregator()
  extends ImpurityAggregator(statsSize = 3) with Serializable {

  /**
    * Update stats for one (node, feature, bin) with the given label.
    * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
    * @param offset    Start index of stats for this (node, feature, bin).
    */
  def update(allStats: Array[Float], offset: Int, label: Float, instanceWeight: Float): Unit = {
    allStats(offset) += instanceWeight
    allStats(offset + 1) += instanceWeight * label
    allStats(offset + 2) += instanceWeight * label * label
  }

  /**
    * Get an [[ImpurityCalculator]] for a (node, feature, bin).
    * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
    * @param offset    Start index of stats for this (node, feature, bin).
    */
  def getCalculator(allStats: Array[Float], offset: Int): VarianceCalculator = {
    new VarianceCalculator(allStats.view(offset, offset + statsSize).toArray)
  }
}

/**
  * Stores statistics for one (node, feature, bin) for calculating impurity.
  * Unlike [[GiniAggregator]], this class stores its own data and is for a specific
  * (node, feature, bin).
  * @param stats  Array of sufficient statistics for a (node, feature, bin).
  */
private class VarianceCalculator(stats: Array[Float]) extends ImpurityCalculator(stats) {

  require(stats.length == 3,
    s"VarianceCalculator requires sufficient statistics array stats to be of length 3," +
      s" but was given array of length ${stats.length}.")

  /**
    * Make a deep copy of this [[ImpurityCalculator]].
    */
  def copy: VarianceCalculator = new VarianceCalculator(stats.clone())

  /**
    * Calculate the impurity from the stored sufficient statistics.
    */
  def calculate(): Float = Variance.calculate(stats(0), stats(1), stats(2))

  /**
    * Number of data points accounted for in the sufficient statistics.
    */
  def count: Long = stats(0).toLong

  /**
    * Prediction which should be made based on the sufficient statistics.
    */
  def predict: Float = if (count == 0) {
    0
  } else {
    stats(1) / count
  }

  override def toString: String = {
    s"VarianceAggregator(cnt = ${stats(0)}, sum = ${stats(1)}, sum2 = ${stats(2)})"
  }

}

