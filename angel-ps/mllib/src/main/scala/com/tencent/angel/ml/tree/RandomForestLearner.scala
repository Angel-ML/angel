package com.tencent.angel.ml.tree

import com.tencent.angel.ml.core.MLLearner
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.model.MLModel

import scala.util.Try
import scala.util.Random
import com.tencent.angel.ml.tree.{TreeEnsembleParams => NewRFParams}
import com.tencent.angel.ml.tree.model.{RandomForest => NewRandomForest}
import com.tencent.angel.ml.tree.conf.Algo._
import com.tencent.angel.ml.tree.conf.QuantileStrategy._
import com.tencent.angel.ml.tree.conf.Strategy
import com.tencent.angel.ml.tree.impurity.Impurities
import com.tencent.angel.ml.tree.oldmodel._
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext

import scala.collection.mutable.ArrayBuffer

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
class RandomForestLearner (
                     override val ctx: TaskContext,
                     val strategy: Strategy,
                     val numTrees: Int,
                     featureSubsetStrategy: String,
                     val seed: Int)
  extends MLLearner(ctx)  {

  strategy.assertValid()
  require(numTrees > 0, s"RandomForest requires numTrees > 0, but was given numTrees = $numTrees.")
  require(RandomForestLearner.supportedFeatureSubsetStrategies.contains(featureSubsetStrategy)
    || Try(featureSubsetStrategy.toInt).filter(_ > 0).isSuccess
    || Try(featureSubsetStrategy.toDouble).filter(_ > 0).filter(_ <= 1.0).isSuccess,
    s"RandomForest given invalid featureSubsetStrategy: $featureSubsetStrategy." +
      s" Supported values: ${NewRFParams.supportedFeatureSubsetStrategies.mkString(", ")}," +
      s" (0.0-1.0], [1-n].")

  private[tree] def convertDataBlock(input: DataBlock[LabeledData]): Array[LabeledData] = {
    input.resetReadIndex()
    val ret = new ArrayBuffer[LabeledData]
    (0 until input.size()).foreach{
      ret += input.read()
    }
    ret.toArray
  }

  /**
    * Method to train a decision tree model over an RDD
    *
    * @param trainBlock Training data: DataBlock of [[org.apache.spark.mllib.regression.LabeledPoint]].
    * @return RandomForestModel that can be used for prediction.
    */
  override
  def train(trainBlock: DataBlock[LabeledData], validBlock: DataBlock[LabeledData]): MLModel = {
    var trainDataSet: Array[LabeledData] = convertDataBlock(trainBlock)
    var validDataSet: Array[LabeledData] = convertDataBlock(validBlock)
    val trees: Array[DecisionTreeModel] = NewRandomForest.train(trainDataSet, validDataSet, strategy, numTrees,
      featureSubsetStrategy, seed.toLong)
    new RandomForestModel(strategy.algo, trees.map(_.toOld), ctx.getConf, ctx)
  }

}

object RandomForestLearner {

  /**
    * Method to train a decision tree model for binary or multiclass classification.
    *
    * @param trainBlock Training dataset: DataBlock of [[LabeledData]].
    *              Labels should take values {0, 1, ..., numClasses-1}.
    * @param strategy Parameters for training each tree in the forest.
    * @param numTrees Number of trees in the random forest.
    * @param featureSubsetStrategy Number of features to consider for splits at each node.
    *                              Supported values: "auto", "all", "sqrt", "log2", "onethird".
    *                              If "auto" is set, this parameter is set based on numTrees:
    *                                if numTrees == 1, set to "all";
    *                                if numTrees is greater than 1 (forest) set to "sqrt".
    * @param seed Random seed for bootstrapping and choosing feature subsets.
    * @return MLModel that can be used for prediction.
    */
  def trainClassifier(
                       ctx: TaskContext,
                       trainBlock: DataBlock[LabeledData],
                       validBlock: DataBlock[LabeledData],
                       strategy: Strategy,
                       numTrees: Int,
                       featureSubsetStrategy: String,
                       seed: Int): MLModel = {
    require(strategy.algo == Classification,
      s"RandomForest.trainClassifier given Strategy with invalid algo: ${strategy.algo}")
    val rf = new RandomForestLearner(ctx, strategy, numTrees, featureSubsetStrategy, seed)
    rf.train(trainBlock, validBlock)
  }

  /**
    * Method to train a decision tree model for binary or multiclass classification.
    *
    * @param trainBlock Training dataset: DataBlock of [[LabeledData]].
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
    * @return MLModel that can be used for prediction.
    */
  def trainClassifier(
                       ctx: TaskContext,
                       trainBlock: DataBlock[LabeledData],
                       validBlock: DataBlock[LabeledData],
                       numClasses: Int,
                       categoricalFeaturesInfo: Map[Int, Int],
                       numTrees: Int,
                       featureSubsetStrategy: String,
                       impurity: String,
                       maxDepth: Int,
                       maxBins: Int,
                       seed: Int = Random.nextInt()): MLModel = {
    val impurityType = Impurities.fromString(impurity)
    val strategy = new Strategy(Classification, impurityType, maxDepth,
      numClasses, maxBins, Sort, categoricalFeaturesInfo)
    trainClassifier(ctx, trainBlock, validBlock, strategy, numTrees, featureSubsetStrategy, seed)
  }

  /**
    * Method to train a decision tree model for regression.
    *
    * @param trainBlock Training dataset: DataBlock of [[LabeledData]].
    *              Labels are real numbers.
    * @param strategy Parameters for training each tree in the forest.
    * @param numTrees Number of trees in the random forest.
    * @param featureSubsetStrategy Number of features to consider for splits at each node.
    *                              Supported values: "auto", "all", "sqrt", "log2", "onethird".
    *                              If "auto" is set, this parameter is set based on numTrees:
    *                                if numTrees == 1, set to "all";
    *                                if numTrees is greater than 1 (forest) set to "onethird".
    * @param seed Random seed for bootstrapping and choosing feature subsets.
    * @return MLModel that can be used for prediction.
    */
  def trainRegressor(
                      ctx: TaskContext,
                      trainBlock: DataBlock[LabeledData],
                      validBlock: DataBlock[LabeledData],
                      strategy: Strategy,
                      numTrees: Int,
                      featureSubsetStrategy: String,
                      seed: Int): MLModel = {
    require(strategy.algo == Regression,
      s"RandomForest.trainRegressor given Strategy with invalid algo: ${strategy.algo}")
    val rf = new RandomForestLearner(ctx, strategy, numTrees, featureSubsetStrategy, seed)
    rf.train(trainBlock, validBlock)
  }

  /**
    * Method to train a decision tree model for regression.
    *
    * @param trainBlock Training dataset: DataBlock of [[LabeledData]].
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
    * @return MLModel that can be used for prediction.
    */
  def trainRegressor(
                      ctx: TaskContext,
                      trainBlock: DataBlock[LabeledData],
                      validBlock: DataBlock[LabeledData],
                      categoricalFeaturesInfo: Map[Int, Int],
                      numTrees: Int,
                      featureSubsetStrategy: String,
                      impurity: String,
                      maxDepth: Int,
                      maxBins: Int,
                      seed: Int = Random.nextInt()): MLModel = {
    val impurityType = Impurities.fromString(impurity)
    val strategy = new Strategy(Regression, impurityType, maxDepth,
      0, maxBins, Sort, categoricalFeaturesInfo)
    trainRegressor(ctx, trainBlock, validBlock, strategy, numTrees, featureSubsetStrategy, seed)
  }

  /**
    * List of supported feature subset sampling strategies.
    */
  val supportedFeatureSubsetStrategies: Array[String] = NewRFParams.supportedFeatureSubsetStrategies
}

