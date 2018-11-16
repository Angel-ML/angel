/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.ml.tree.conf

import scala.beans.BeanProperty
import scala.collection.JavaConverters._
import com.tencent.angel.ml.tree.conf.Algo._
import com.tencent.angel.ml.tree.conf.QuantileStrategy._
import com.tencent.angel.ml.tree.impurity.{Entropy, Gini, Impurity, Variance}

import scala.util.Try

/**
  * Stores all the configuration options for tree construction
  * @param algo  Learning goal.  Supported: Classification, Regression
  * @param impurity Criterion used for information gain calculation.
  *                 Supported for Classification: Gini, Entropy. Supported for Regression: Variance
  * @param numTrees If 1, then no bootstrapping is used.  If greater than 1, then bootstrapping is done.
  * @param maxDepth Maximum depth of the tree (e.g. depth 0 means 1 leaf node,
  *                 depth 1 means 1 internal node + 2 leaf nodes).
  * @param numClasses Number of classes for classification. (Ignored for regression.)
  *                   Default value is 2 (binary classification).
  * @param maxBins Maximum number of bins used for discretizing continuous features and
  *                for choosing how to split on features at each node.
  * @param subsamplingRate Fraction of the training data used for learning decision tree.
  * @param featureSubsetStrategy Number of features to consider for splits at each node.
  *                              Supported values: "auto", "all", "sqrt", "log2", "onethird".
  *                              Supported numerical values: "(0.0-1.0]", "[1-n]".
  *                              If "auto" is set, this parameter is set based on numTrees:
  *                                if numTrees == 1, set to "all";
  *                                if numTrees is greater than 1 (forest) set to "sqrt" for
  *                                  classification and to "onethird" for regression.
  *                              If a real value "n" in the range (0, 1.0] is set,
  *                                use n * number of features.
  *                              If an integer value "n" in the range (1, num features) is set,
  *                                use n features.
  * @param quantileCalculationStrategy Algorithm for calculating quantiles.
  *                                    Supported: QuantileStrategy.Sort
  * @param categoricalFeaturesInfo A map storing information about the categorical variables
  *                                and the number of discrete values they take. An entry (n to k) indicates that
  *                                feature n is categorical with k categories indexed from 0: {0, 1, ..., k-1}.
  * @param minInstancesPerNode Minimum number of instances each child must have after split. Default value is 1.
  *                            If a split cause left or right child to have less than minInstancesPerNode,
  *                            this split will not be considered as a valid split.
  * @param minInfoGain Minimum information gain a split must get. Default value is 0.0.
  *                    If a split has less information gain than minInfoGain,
  *                    this split will not be considered as a valid split.
  * @param maxMemoryInMB Maximum memory in MB allocated to histogram aggregation. Default value is 256 MB.
  *                      If too small, then 1 node will be split per iteration,
  *                      nd its aggregates may exceed this size.
  * @param useNodeIdCache If this is true, instead of passing trees to executors,
  *                       the algorithm will maintain a separate RDD of node Id cache for each row.
  * @param checkpointInterval How often to checkpoint when the node Id cache gets updated.
  *                           E.g. 10 means that the cache will get checkpointed every 10 update.
  */
class Strategy (@BeanProperty var algo: Algo,
                @BeanProperty  var impurity: Impurity,
                @BeanProperty var numTrees: Int = 1,
                @BeanProperty var maxDepth: Int = 2,
                @BeanProperty var numClasses: Int = 2,
                @BeanProperty var maxBins: Int = 32,
                @BeanProperty var subsamplingRate: Double = 1,
                @BeanProperty var featureSubsetStrategy: String = "auto",
                var quantileCalculationStrategy: QuantileStrategy = Sort,
                var categoricalFeaturesInfo: Map[Int, Int] = Map[Int, Int](),
                var minInstancesPerNode: Int = 1,
                var minInfoGain: Double = 0.0,
                var maxMemoryInMB: Int = 256,
                var useNodeIdCache: Boolean = false,
                var checkpointInterval: Int = 10) extends Serializable {

  def isMulticlassClassification: Boolean = {
    algo == Classification && numClasses > 2
  }

  def isMulticlassWithCategoricalFeatures: Boolean = {
    isMulticlassClassification && categoricalFeaturesInfo.nonEmpty
  }

  def this(
            algo: Algo,
            impurity: Impurity,
            numTrees: Int,
            maxDepth: Int,
            numClasses: Int,
            maxBins: Int,
            subsamplingRate: Double,
            featureSubsetStrategy: String,
            categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer]) {
    this(algo, impurity, numTrees, maxDepth, numClasses, maxBins, subsamplingRate, featureSubsetStrategy, Sort,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap)
  }

  /**
    * Sets Algorithm using a String.
    */
  def setAlgo(algo: String): Unit = algo match {
    case "Classification" => setAlgo(Classification)
    case "Regression" => setAlgo(Regression)
  }

  /**
    * Sets categoricalFeaturesInfo using a Java Map.
    */
  def setCategoricalFeaturesInfo(
                                  categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer]): Unit = {
    this.categoricalFeaturesInfo =
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap
  }

  /**
    * Check validity of parameters.
    * Throws exception if invalid.
    */
  def assertValid(): Unit = {
    algo match {
      case Classification =>
        require(numClasses >= 2,
          s"DecisionTree Strategy for Classification must have numClasses >= 2," +
            s" but numClasses = $numClasses.")
        require(Set(Gini, Entropy).contains(impurity),
          s"DecisionTree Strategy given invalid impurity for Classification: $impurity." +
            s"  Valid settings: Gini, Entropy")
      case Regression =>
        require(impurity == Variance,
          s"DecisionTree Strategy given invalid impurity for Regression: $impurity." +
            s"  Valid settings: Variance")
      case _ =>
        throw new IllegalArgumentException(
          s"DecisionTree Strategy given invalid algo parameter: $algo." +
            s"  Valid settings are: Classification, Regression.")
    }
    require(maxDepth >= 0, s"DecisionTree Strategy given invalid maxDepth parameter: $maxDepth." +
      s"  Valid values are integers >= 0.")
    require(maxBins >= 2, s"DecisionTree Strategy given invalid maxBins parameter: $maxBins." +
      s"  Valid values are integers >= 2.")
    require(minInstancesPerNode >= 1,
      s"DecisionTree Strategy requires minInstancesPerNode >= 1 but was given $minInstancesPerNode")
    require(maxMemoryInMB <= 10240,
      s"DecisionTree Strategy requires maxMemoryInMB <= 10240, but was given $maxMemoryInMB")
    require(subsamplingRate > 0 && subsamplingRate <= 1,
      s"DecisionTree Strategy requires subsamplingRate <=1 and >0, but was given " +
        s"$subsamplingRate")
    require(numTrees > 0, s"RandomForest requires numTrees > 0, but was given numTrees = $numTrees.")
    require(Strategy.supportedFeatureSubsetStrategies.contains(featureSubsetStrategy)
      || Try(featureSubsetStrategy.toInt).filter(_ > 0).isSuccess
      || Try(featureSubsetStrategy.toDouble).filter(_ > 0).filter(_ <= 1.0).isSuccess,
      s"RandomForest given invalid featureSubsetStrategy: $featureSubsetStrategy." +
        s" Supported values: ${Strategy.supportedFeatureSubsetStrategies.mkString(", ")}," +
        s" (0.0-1.0], [1-n].")
  }

  /**
    * Returns a shallow copy of this instance.
    */
  def copy: Strategy = {
    new Strategy(algo, impurity, maxDepth, numClasses, maxBins,
      quantileCalculationStrategy, categoricalFeaturesInfo, minInstancesPerNode, minInfoGain,
      maxMemoryInMB, subsamplingRate, useNodeIdCache, checkpointInterval)
  }
}

object Strategy {

  /**
    * List of supported feature subset sampling strategies.
    */
  val supportedFeatureSubsetStrategies: Array[String] = NewRFParams.supportedFeatureSubsetStrategies

  /**
    * Construct a default set of parameters for [[org.apache.spark.mllib.tree.DecisionTree]]
    * @param algo  "Classification" or "Regression"
    */
  def defaultStrategy(algo: String): Strategy = {
    defaultStrategy(Algo.fromString(algo))
  }

  /**
    * Construct a default set of parameters for DecisionTree
    * @param Classification or Regression
    */
  def defaultStrategy(algo: Algo): Strategy = algo match {
    case Algo.Classification =>
      new Strategy(algo = Classification, impurity = Gini, maxDepth = 10, numClasses = 2)
    case Algo.Regression =>
      new Strategy(algo = Regression, impurity = Variance, maxDepth = 10, numClasses = 0)
  }

}
