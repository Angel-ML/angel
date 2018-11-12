package com.tencent.angel.ml.tree.impurity

import java.util.Locale

/**
  * Trait for calculating information gain.
  * This trait is used for
  *  (a) setting the impurity parameter in [[org.apache.spark.mllib.tree.configuration.Strategy]]
  *  (b) calculating impurity values from sufficient statistics.
  */
trait Impurity extends Serializable {

  /**
    * information calculation for multiclass classification
    * @param counts Array[Double] with counts for each label
    * @param totalCount sum of counts for all labels
    * @return information value, or 0 if totalCount = 0
    */
  def calculate(counts: Array[Float], totalCount: Float): Float

  /**
    * information calculation for regression
    * @param count number of instances
    * @param sum sum of labels
    * @param sumSquares summation of squares of the labels
    * @return information value, or 0 if count = 0
    */
  def calculate(count: Float, sum: Float, sumSquares: Float): Float
}

/**
  * Interface for updating views of a vector of sufficient statistics,
  * in order to compute impurity from a sample.
  * Note: Instances of this class do not hold the data; they operate on views of the data.
  * @param statsSize  Length of the vector of sufficient statistics for one bin.
  */
private abstract class ImpurityAggregator(val statsSize: Int) extends Serializable {

  /**
    * Merge the stats from one bin into another.
    * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
    * @param offset    Start index of stats for (node, feature, bin) which is modified by the merge.
    * @param otherOffset  Start index of stats for (node, feature, other bin) which is not modified.
    */
  def merge(allStats: Array[Float], offset: Int, otherOffset: Int): Unit = {
    var i = 0
    while (i < statsSize) {
      allStats(offset + i) += allStats(otherOffset + i)
      i += 1
    }
  }

  /**
    * Update stats for one (node, feature, bin) with the given label.
    * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
    * @param offset    Start index of stats for this (node, feature, bin).
    */
  def update(allStats: Array[Float], offset: Int, label: Float, instanceWeight: Float): Unit

  /**
    * Get an [[ImpurityCalculator]] for a (node, feature, bin).
    * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
    * @param offset    Start index of stats for this (node, feature, bin).
    */
  def getCalculator(allStats: Array[Float], offset: Int): ImpurityCalculator
}

/**
  * Stores statistics for one (node, feature, bin) for calculating impurity.
  * Unlike [[ImpurityAggregator]], this class stores its own data and is for a specific
  * (node, feature, bin).
  * @param stats  Array of sufficient statistics for a (node, feature, bin).
  */
private[tree] abstract class ImpurityCalculator(val stats: Array[Float]) extends Serializable {

  /**
    * Make a deep copy of this [[ImpurityCalculator]].
    */
  def copy: ImpurityCalculator

  /**
    * Calculate the impurity from the stored sufficient statistics.
    */
  def calculate(): Float

  /**
    * Add the stats from another calculator into this one, modifying and returning this calculator.
    */
  def add(other: ImpurityCalculator): ImpurityCalculator = {
    require(stats.length == other.stats.length,
      s"Two ImpurityCalculator instances cannot be added with different counts sizes." +
        s"  Sizes are ${stats.length} and ${other.stats.length}.")
    var i = 0
    val len = other.stats.length
    while (i < len) {
      stats(i) += other.stats(i)
      i += 1
    }
    this
  }

  /**
    * Subtract the stats from another calculator from this one, modifying and returning this
    * calculator.
    */
  def subtract(other: ImpurityCalculator): ImpurityCalculator = {
    require(stats.length == other.stats.length,
      s"Two ImpurityCalculator instances cannot be subtracted with different counts sizes." +
        s"  Sizes are ${stats.length} and ${other.stats.length}.")
    var i = 0
    val len = other.stats.length
    while (i < len) {
      stats(i) -= other.stats(i)
      i += 1
    }
    this
  }

  /**
    * Number of data points accounted for in the sufficient statistics.
    */
  def count: Long

  /**
    * Prediction which should be made based on the sufficient statistics.
    */
  def predict: Float

  /**
    * Probability of the label given by [[predict]], or -1 if no probability is available.
    */
  def prob(label: Float): Float = -1

  /**
    * Return the index of the largest array element.
    * Fails if the array is empty.
    */
  protected def indexOfLargestArrayElement(array: Array[Float]): Int = {
    val result = array.foldLeft((-1, Double.MinValue, 0)) {
      case ((maxIndex, maxValue, currentIndex), currentValue) =>
        if (currentValue > maxValue) {
          (currentIndex, currentValue, currentIndex + 1)
        } else {
          (maxIndex, maxValue, currentIndex + 1)
        }
    }
    if (result._1 < 0) {
      throw new RuntimeException("ImpurityCalculator internal error:" +
        " indexOfLargestArrayElement failed")
    }
    result._1
  }

}

private object ImpurityCalculator {

  /**
    * Create an [[ImpurityCalculator]] instance of the given impurity type and with
    * the given stats.
    */
  def getCalculator(impurity: String, stats: Array[Float]): ImpurityCalculator = {
    impurity.toLowerCase(Locale.ROOT) match {
      case "gini" => new GiniCalculator(stats)
      case "entropy" => new EntropyCalculator(stats)
      case "variance" => new VarianceCalculator(stats)
      case _ =>
        throw new IllegalArgumentException(
          s"ImpurityCalculator builder did not recognize impurity type: $impurity")
    }
  }
}