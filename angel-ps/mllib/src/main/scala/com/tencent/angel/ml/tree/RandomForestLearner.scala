package com.tencent.angel.ml.tree

import com.tencent.angel.ml.core.MLLearner
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.model.MLModel

import scala.util.Try

import org.apache.spark.ml.tree.{DecisionTreeModel => NewDTModel, TreeEnsembleParams => NewRFParams}
import org.apache.spark.ml.tree.impl.{RandomForest => NewRandomForest}
import com.tencent.angel.ml.tree.conf.Algo._
import com.tencent.angel.ml.tree.conf.QuantileStrategy._
import com.tencent.angel.ml.tree.conf.Strategy
import com.tencent.angel.ml.tree.impurity.Impurities
import com.tencent.angel.ml.tree.model._
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext

/**
  * A class that implements a <a href="http://en.wikipedia.org/wiki/Random_forest">Random Forest</a>
  * learning algorithm for classification and regression.
  * It supports both continuous and categorical features.
  *
  * The settings for featureSubsetStrategy are based on the following references:
  *  - log2: tested in Breiman (2001)
  *  - sqrt: recommended by Breiman manual for random forests
  *  - The defaults of sqrt (classification) and onethird (regression) match the R randomForest
  *    package.
  *
  * @see <a href="http://www.stat.berkeley.edu/~breiman/randomforest2001.pdf">Breiman (2001)</a>
  * @see <a href="http://www.stat.berkeley.edu/~breiman/Using_random_forests_V3.1.pdf">
  * Breiman manual for random forests</a>
  * @param strategy The configuration parameters for the random forest algorithm which specify
  *                 the type of random forest (classification or regression), feature type
  *                 (continuous, categorical), depth of the tree, quantile calculation strategy,
  *                 etc.
  * @param numTrees If 1, then no bootstrapping is used.  If greater than 1, then bootstrapping is
  *                 done.
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
  * @param seed Random seed for bootstrapping and choosing feature subsets.
  */
class RandomForest (
                             val ctx: TaskContext,
                             val strategy: Strategy,
                             val numTrees: Int,
                             featureSubsetStrategy: String,
                             val seed: Int)
  extends MLLearner(ctx)  {

  strategy.assertValid()
  require(numTrees > 0, s"RandomForest requires numTrees > 0, but was given numTrees = $numTrees.")
  require(RandomForest.supportedFeatureSubsetStrategies.contains(featureSubsetStrategy)
    || Try(featureSubsetStrategy.toInt).filter(_ > 0).isSuccess
    || Try(featureSubsetStrategy.toDouble).filter(_ > 0).filter(_ <= 1.0).isSuccess,
    s"RandomForest given invalid featureSubsetStrategy: $featureSubsetStrategy." +
      s" Supported values: ${NewRFParams.supportedFeatureSubsetStrategies.mkString(", ")}," +
      s" (0.0-1.0], [1-n].")

  /**
    * Method to train a decision tree model over an RDD
    *
    * @param input Training data: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
    * @return RandomForestModel that can be used for prediction.
    */
  override
  def train(trainData: DataBlock[LabeledData], validationData: DataBlock[LabeledData]): MLModel = {
    val trees: Array[NewDTModel] = NewRandomForest.run(input.map(_.asML), strategy, numTrees,
      featureSubsetStrategy, seed.toLong, None)
    new RandomForestModel(strategy.algo, trees.map(_.toOld))
  }

}

object RandomForest {

  /**
    * Method to train a decision tree model for binary or multiclass classification.
    *
    * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
    *              Labels should take values {0, 1, ..., numClasses-1}.
    * @param strategy Parameters for training each tree in the forest.
    * @param numTrees Number of trees in the random forest.
    * @param featureSubsetStrategy Number of features to consider for splits at each node.
    *                              Supported values: "auto", "all", "sqrt", "log2", "onethird".
    *                              If "auto" is set, this parameter is set based on numTrees:
    *                                if numTrees == 1, set to "all";
    *                                if numTrees is greater than 1 (forest) set to "sqrt".
    * @param seed Random seed for bootstrapping and choosing feature subsets.
    * @return RandomForestModel that can be used for prediction.
    */
  def trainClassifier(
                       input: RDD[LabeledPoint],
                       strategy: Strategy,
                       numTrees: Int,
                       featureSubsetStrategy: String,
                       seed: Int): RandomForestModel = {
    require(strategy.algo == Classification,
      s"RandomForest.trainClassifier given Strategy with invalid algo: ${strategy.algo}")
    val rf = new RandomForest(strategy, numTrees, featureSubsetStrategy, seed)
    rf.run(input)
  }

  /**
    * Method to train a decision tree model for binary or multiclass classification.
    *
    * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
    *              Labels should take values {0, 1, ..., numClasses-1}.
    * @param numClasses Number of classes for classification.
    * @param categoricalFeaturesInfo Map storing arity of categorical features. An entry (n to k)
    *                                indicates that feature n is categorical with k categories
    *                                indexed from 0: {0, 1, ..., k-1}.
    * @param numTrees Number of trees in the random forest.
    * @param featureSubsetStrategy Number of features to consider for splits at each node.
    *                              Supported values: "auto", "all", "sqrt", "log2", "onethird".
    *                              If "auto" is set, this parameter is set based on numTrees:
    *                                if numTrees == 1, set to "all";
    *                                if numTrees is greater than 1 (forest) set to "sqrt".
    * @param impurity Criterion used for information gain calculation.
    *                 Supported values: "gini" (recommended) or "entropy".
    * @param maxDepth Maximum depth of the tree (e.g. depth 0 means 1 leaf node, depth 1 means
    *                 1 internal node + 2 leaf nodes).
    *                 (suggested value: 4)
    * @param maxBins Maximum number of bins used for splitting features
    *                (suggested value: 100)
    * @param seed Random seed for bootstrapping and choosing feature subsets.
    * @return RandomForestModel that can be used for prediction.
    */
  @Since("1.2.0")
  def trainClassifier(
                       input: RDD[LabeledPoint],
                       numClasses: Int,
                       categoricalFeaturesInfo: Map[Int, Int],
                       numTrees: Int,
                       featureSubsetStrategy: String,
                       impurity: String,
                       maxDepth: Int,
                       maxBins: Int,
                       seed: Int = Utils.random.nextInt()): RandomForestModel = {
    val impurityType = Impurities.fromString(impurity)
    val strategy = new Strategy(Classification, impurityType, maxDepth,
      numClasses, maxBins, Sort, categoricalFeaturesInfo)
    trainClassifier(input, strategy, numTrees, featureSubsetStrategy, seed)
  }

  /**
    * Java-friendly API for `org.apache.spark.mllib.tree.RandomForest.trainClassifier`
    */
  @Since("1.2.0")
  def trainClassifier(
                       input: JavaRDD[LabeledPoint],
                       numClasses: Int,
                       categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
                       numTrees: Int,
                       featureSubsetStrategy: String,
                       impurity: String,
                       maxDepth: Int,
                       maxBins: Int,
                       seed: Int): RandomForestModel = {
    trainClassifier(input.rdd, numClasses,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed)
  }

  /**
    * Method to train a decision tree model for regression.
    *
    * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
    *              Labels are real numbers.
    * @param strategy Parameters for training each tree in the forest.
    * @param numTrees Number of trees in the random forest.
    * @param featureSubsetStrategy Number of features to consider for splits at each node.
    *                              Supported values: "auto", "all", "sqrt", "log2", "onethird".
    *                              If "auto" is set, this parameter is set based on numTrees:
    *                                if numTrees == 1, set to "all";
    *                                if numTrees is greater than 1 (forest) set to "onethird".
    * @param seed Random seed for bootstrapping and choosing feature subsets.
    * @return RandomForestModel that can be used for prediction.
    */
  @Since("1.2.0")
  def trainRegressor(
                      input: RDD[LabeledPoint],
                      strategy: Strategy,
                      numTrees: Int,
                      featureSubsetStrategy: String,
                      seed: Int): RandomForestModel = {
    require(strategy.algo == Regression,
      s"RandomForest.trainRegressor given Strategy with invalid algo: ${strategy.algo}")
    val rf = new RandomForest(strategy, numTrees, featureSubsetStrategy, seed)
    rf.run(input)
  }

  /**
    * Method to train a decision tree model for regression.
    *
    * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
    *              Labels are real numbers.
    * @param categoricalFeaturesInfo Map storing arity of categorical features. An entry (n to k)
    *                                indicates that feature n is categorical with k categories
    *                                indexed from 0: {0, 1, ..., k-1}.
    * @param numTrees Number of trees in the random forest.
    * @param featureSubsetStrategy Number of features to consider for splits at each node.
    *                              Supported values: "auto", "all", "sqrt", "log2", "onethird".
    *                              If "auto" is set, this parameter is set based on numTrees:
    *                                if numTrees == 1, set to "all";
    *                                if numTrees is greater than 1 (forest) set to "onethird".
    * @param impurity Criterion used for information gain calculation.
    *                 The only supported value for regression is "variance".
    * @param maxDepth Maximum depth of the tree. (e.g., depth 0 means 1 leaf node, depth 1 means
    *                 1 internal node + 2 leaf nodes).
    *                 (suggested value: 4)
    * @param maxBins Maximum number of bins used for splitting features.
    *                (suggested value: 100)
    * @param seed Random seed for bootstrapping and choosing feature subsets.
    * @return RandomForestModel that can be used for prediction.
    */
  @Since("1.2.0")
  def trainRegressor(
                      input: RDD[LabeledPoint],
                      categoricalFeaturesInfo: Map[Int, Int],
                      numTrees: Int,
                      featureSubsetStrategy: String,
                      impurity: String,
                      maxDepth: Int,
                      maxBins: Int,
                      seed: Int = Utils.random.nextInt()): RandomForestModel = {
    val impurityType = Impurities.fromString(impurity)
    val strategy = new Strategy(Regression, impurityType, maxDepth,
      0, maxBins, Sort, categoricalFeaturesInfo)
    trainRegressor(input, strategy, numTrees, featureSubsetStrategy, seed)
  }

  /**
    * Java-friendly API for `org.apache.spark.mllib.tree.RandomForest.trainRegressor`
    */
  @Since("1.2.0")
  def trainRegressor(
                      input: JavaRDD[LabeledPoint],
                      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
                      numTrees: Int,
                      featureSubsetStrategy: String,
                      impurity: String,
                      maxDepth: Int,
                      maxBins: Int,
                      seed: Int): RandomForestModel = {
    trainRegressor(input.rdd,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed)
  }

  /**
    * List of supported feature subset sampling strategies.
    */
  @Since("1.2.0")
  val supportedFeatureSubsetStrategies: Array[String] = NewRFParams.supportedFeatureSubsetStrategies
}

