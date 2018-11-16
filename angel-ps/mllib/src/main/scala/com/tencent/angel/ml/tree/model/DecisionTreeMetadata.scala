package com.tencent.angel.ml.tree.model

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.vector.IntFloatVector
import com.tencent.angel.ml.tree.TreeEnsembleParams

import scala.collection.mutable
import scala.util.Try
import com.tencent.angel.ml.tree.conf.Algo._
import com.tencent.angel.ml.tree.conf.QuantileStrategy._
import com.tencent.angel.ml.tree.conf.Strategy
import com.tencent.angel.ml.tree.impurity.Impurity
import org.apache.commons.logging.LogFactory

/**
  * Learning and dataset metadata for DecisionTree.
  *
  * @param numClasses    For classification: labels can take values {0, ..., numClasses - 1}.
  *                      For regression: fixed at 0 (no meaning).
  * @param maxBins  Maximum number of bins, for all features.
  * @param featureArity  Map: categorical feature index to arity.
  *                      I.e., the feature takes values in {0, ..., arity - 1}.
  * @param numBins  Number of bins for each feature.
  */
private[tree] class DecisionTreeMetadata(
                                           val numFeatures: Int,
                                           val numExamples: Long,
                                           val numClasses: Int,
                                           val maxBins: Int,
                                           val featureArity: Map[Int, Int],
                                           val unorderedFeatures: Set[Int],
                                           val numBins: Array[Int],
                                           val impurity: Impurity,
                                           val quantileStrategy: QuantileStrategy,
                                           val maxDepth: Int,
                                           val minInstancesPerNode: Int,
                                           val minInfoGain: Double,
                                           val numTrees: Int,
                                           val numFeaturesPerNode: Int) extends Serializable {

  def isUnordered(featureIndex: Int): Boolean = unorderedFeatures.contains(featureIndex)

  def isClassification: Boolean = numClasses >= 2

  def isMulticlass: Boolean = numClasses > 2

  def isMulticlassWithCategoricalFeatures: Boolean = isMulticlass && featureArity.nonEmpty

  def isCategorical(featureIndex: Int): Boolean = featureArity.contains(featureIndex)

  def isContinuous(featureIndex: Int): Boolean = !featureArity.contains(featureIndex)

  /**
    * Number of splits for the given feature.
    * For unordered features, there is 1 bin per split.
    * For ordered features, there is 1 more bin than split.
    */
  def numSplits(featureIndex: Int): Int = if (isUnordered(featureIndex)) {
    numBins(featureIndex)
  } else {
    numBins(featureIndex) - 1
  }

  /**
    * Set number of splits for a continuous feature.
    * For a continuous feature, number of bins is number of splits plus 1.
    */
  def setNumSplits(featureIndex: Int, numSplits: Int) {
    require(isContinuous(featureIndex),
      s"Only number of bin for a continuous feature can be set.")
    numBins(featureIndex) = numSplits + 1
  }

  /**
    * Indicates if feature subsampling is being used.
    */
  def subsamplingFeatures: Boolean = numFeatures != numFeaturesPerNode

}

private[tree] object DecisionTreeMetadata {

  val LOG = LogFactory.getLog(classOf[DecisionTreeMetadata])

  /**
    * Construct a [[DecisionTreeMetadata]] instance for this dataset and parameters.
    * This computes which categorical features will be ordered vs. unordered,
    * as well as the number of splits and bins for each feature.
    */
  def buildMetadata(
                     input: Array[LabeledData],
                     strategy: Strategy): DecisionTreeMetadata = {

    val numFeatures = input(0).getX.asInstanceOf[IntFloatVector].getDim
    if (numFeatures == 0)
      throw new IllegalArgumentException(s"DecisionTree requires size of input RDD > 0, " +
        s"but was given by empty one.")
    require(numFeatures > 0, s"DecisionTree requires number of features > 0, " +
      s"but was given an empty features vector")
    val numExamples = input.length
    val numClasses = strategy.algo match {
      case Classification => strategy.numClasses
      case Regression => 0
    }

    val maxPossibleBins = math.min(strategy.maxBins, numExamples)
    if (maxPossibleBins < strategy.maxBins) {
      LOG.error(s"DecisionTree reducing maxBins from ${strategy.maxBins} to $maxPossibleBins" +
        s" (= number of training instances)")
    }

    // We check the number of bins here against maxPossibleBins.
    // This needs to be checked here instead of in Strategy since maxPossibleBins can be modified
    // based on the number of training examples.
    if (strategy.categoricalFeaturesInfo.nonEmpty) {
      val maxCategoriesPerFeature = strategy.categoricalFeaturesInfo.values.max
      val maxCategory =
        strategy.categoricalFeaturesInfo.find(_._2 == maxCategoriesPerFeature).get._1
      require(maxCategoriesPerFeature <= maxPossibleBins,
        s"DecisionTree requires maxBins (= $maxPossibleBins) to be at least as large as the " +
          s"number of values in each categorical feature, but categorical feature $maxCategory " +
          s"has $maxCategoriesPerFeature values. Considering remove this and other categorical " +
          "features with a large number of values, or add more training examples.")
    }

    val unorderedFeatures = new mutable.HashSet[Int]()
    val numBins = Array.fill[Int](numFeatures)(maxPossibleBins)
    if (numClasses > 2) {
      // Multiclass classification
      val maxCategoriesForUnorderedFeature =
        ((math.log(maxPossibleBins / 2 + 1) / math.log(2.0)) + 1).floor.toInt
      strategy.categoricalFeaturesInfo.foreach { case (featureIndex, numCategories) =>
        // Hack: If a categorical feature has only 1 category, we treat it as continuous.
        // TODO(SPARK-9957): Handle this properly by filtering out those features.
        if (numCategories > 1) {
          // Decide if some categorical features should be treated as unordered features,
          //  which require 2 * ((1 << numCategories - 1) - 1) bins.
          // We do this check with log values to prevent overflows in case numCategories is large.
          // The next check is equivalent to: 2 * ((1 << numCategories - 1) - 1) <= maxBins
          if (numCategories <= maxCategoriesForUnorderedFeature) {
            unorderedFeatures.add(featureIndex)
            numBins(featureIndex) = numUnorderedBins(numCategories)
          } else {
            numBins(featureIndex) = numCategories
          }
        }
      }
    } else {
      // Binary classification or regression
      strategy.categoricalFeaturesInfo.foreach { case (featureIndex, numCategories) =>
        // If a categorical feature has only 1 category, we treat it as continuous: SPARK-9957
        if (numCategories > 1) {
          numBins(featureIndex) = numCategories
        }
      }
    }

    // Set number of features to use per node (for random forests).
    val _featureSubsetStrategy = strategy.featureSubsetStrategy match {
      case "auto" =>
        if (strategy.numTrees == 1) {
          "all"
        } else {
          if (strategy.algo == Classification) {
            "sqrt"
          } else {
            "onethird"
          }
        }
      case _ => strategy.featureSubsetStrategy
    }

    val numFeaturesPerNode: Int = _featureSubsetStrategy match {
      case "all" => numFeatures
      case "sqrt" => math.sqrt(numFeatures).ceil.toInt
      case "log2" => math.max(1, (math.log(numFeatures) / math.log(2)).ceil.toInt)
      case "onethird" => (numFeatures / 3.0).ceil.toInt
      case _ =>
        Try(_featureSubsetStrategy.toInt).filter(_ > 0).toOption match {
          case Some(value) => math.min(value, numFeatures)
          case None =>
            Try(_featureSubsetStrategy.toDouble).filter(_ > 0).filter(_ <= 1.0).toOption match {
              case Some(value) => math.ceil(value * numFeatures).toInt
              case _ => throw new IllegalArgumentException(s"Supported values:" +
                s" ${TreeEnsembleParams.supportedFeatureSubsetStrategies.mkString(", ")}," +
                s" (0.0-1.0], [1-n].")
            }
        }
    }

    new DecisionTreeMetadata(numFeatures, numExamples, numClasses, numBins.max,
      strategy.categoricalFeaturesInfo, unorderedFeatures.toSet, numBins,
      strategy.impurity, strategy.quantileCalculationStrategy, strategy.maxDepth,
      strategy.minInstancesPerNode, strategy.minInfoGain, strategy.numTrees, numFeaturesPerNode)
  }

  /**
    * Given the arity of a categorical feature (arity = number of categories),
    * return the number of bins for the feature if it is to be treated as an unordered feature.
    * There is 1 split for every partitioning of categories into 2 disjoint, non-empty sets;
    * there are math.pow(2, arity - 1) - 1 such splits.
    * Each split has 2 corresponding bins.
    */
  def numUnorderedBins(arity: Int): Int = (1 << arity - 1) - 1

}

