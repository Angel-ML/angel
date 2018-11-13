package com.tencent.angel.ml.tree.impurity

/**
  * Class for calculating entropy during multiclass classification.
  */
object Entropy extends Impurity {

  private def log2(x: Float) = scala.math.log(x) / scala.math.log(2)

  /**
    * information calculation for multiclass classification
    * @param counts Array[Double] with counts for each label
    * @param totalCount sum of counts for all labels
    * @return information value, or 0 if totalCount = 0
    */
  override def calculate(counts: Array[Float], totalCount: Float): Float = {
    if (totalCount == 0) {
      return 0
    }
    val numClasses = counts.length
    var impurity = 0.0
    var classIndex = 0
    while (classIndex < numClasses) {
      val classCount = counts(classIndex)
      if (classCount != 0) {
        val freq = classCount / totalCount
        impurity -= freq * log2(freq)
      }
      classIndex += 1
    }
    impurity.toFloat
  }

  /**
    * variance calculation
    * @param count number of instances
    * @param sum sum of labels
    * @param sumSquares summation of squares of the labels
    * @return information value, or 0 if count = 0
    */
  override def calculate(count: Float, sum: Float, sumSquares: Float): Float =
    throw new UnsupportedOperationException("Entropy.calculate")

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
  * @param numClasses  Number of classes for label.
  */
private class EntropyAggregator(numClasses: Int)
  extends ImpurityAggregator(numClasses) with Serializable {

  /**
    * Update stats for one (node, feature, bin) with the given label.
    * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
    * @param offset    Start index of stats for this (node, feature, bin).
    */
  def update(allStats: Array[Float], offset: Int, label: Float, instanceWeight: Float): Unit = {
    if (label >= statsSize) {
      throw new IllegalArgumentException(s"EntropyAggregator given label $label" +
        s" but requires label < numClasses (= $statsSize).")
    }
    if (label < 0) {
      throw new IllegalArgumentException(s"EntropyAggregator given label $label" +
        s"but requires label is non-negative.")
    }
    allStats(offset + label.toInt) += instanceWeight
  }

  /**
    * Get an [[ImpurityCalculator]] for a (node, feature, bin).
    * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
    * @param offset    Start index of stats for this (node, feature, bin).
    */
  def getCalculator(allStats: Array[Float], offset: Int): EntropyCalculator = {
    new EntropyCalculator(allStats.view(offset, offset + statsSize).toArray)
  }
}

/**
  * Stores statistics for one (node, feature, bin) for calculating impurity.
  * Unlike [[EntropyAggregator]], this class stores its own data and is for a specific
  * (node, feature, bin).
  * @param stats  Array of sufficient statistics for a (node, feature, bin).
  */
private class EntropyCalculator(stats: Array[Float]) extends ImpurityCalculator(stats) {

  /**
    * Make a deep copy of this [[ImpurityCalculator]].
    */
  def copy: EntropyCalculator = new EntropyCalculator(stats.clone())

  /**
    * Calculate the impurity from the stored sufficient statistics.
    */
  def calculate(): Float = Entropy.calculate(stats, stats.sum)

  /**
    * Number of data points accounted for in the sufficient statistics.
    */
  def count: Long = stats.sum.toLong

  /**
    * Prediction which should be made based on the sufficient statistics.
    */
  def predict: Float = if (count == 0) {
    0
  } else {
    indexOfLargestArrayElement(stats)
  }

  /**
    * Probability of the label given by [[predict]].
    */
  override def prob(label: Float): Float = {
    val lbl = label.toInt
    require(lbl < stats.length,
      s"EntropyCalculator.prob given invalid label: $lbl (should be < ${stats.length}")
    require(lbl >= 0, "Entropy does not support negative labels")
    val cnt = count
    if (cnt == 0) {
      0
    } else {
      stats(lbl) / cnt
    }
  }

  override def toString: String = s"EntropyCalculator(stats = [${stats.mkString(", ")}])"

}
