package com.tencent.angel.ml.tree

import com.tencent.angel.ml.core.MLLearner
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.ml.tree.conf.Algo._
import com.tencent.angel.ml.tree.conf.QuantileStrategy._
import com.tencent.angel.ml.tree.conf.{Algo, Strategy}
import com.tencent.angel.ml.tree.impurity._
import com.tencent.angel.ml.tree.oldmodel._
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext

/**
  * A class which implements a decision tree learning algorithm for classification and regression.
  * It supports both continuous and categorical features.
  *
  * @param strategy The configuration parameters for the tree algorithm which specify the type
  *                 of decision tree (classification or regression), feature type (continuous,
  *                 categorical), depth of the tree, quantile calculation strategy, etc.
  * @param seed Random seed.
  */
class DecisionTreeLearner (ctx: TaskContext, val strategy: Strategy, val seed: Int) extends MLLearner(ctx) {

  /**
    * @param strategy The configuration parameters for the tree algorithm which specify the type
    *                 of decision tree (classification or regression), feature type (continuous,
    *                 categorical), depth of the tree, quantile calculation strategy, etc.
    */
  def this(ctx: TaskContext, strategy: Strategy) = this(ctx, strategy, seed = 0)
  def this(ctx: TaskContext) = this(ctx, Strategy.defaultStrategy(Algo.Classification))

  strategy.assertValid()

  /**
    * Method to train a decision tree model over an RDD
    *
    * @param trainBlock : trainning data storage
    * @param validBlock : validation data storage
    */
  override
  def train(trainBlock: DataBlock[LabeledData], validBlock: DataBlock[LabeledData]): MLModel = {
    val rf = new RandomForestLearner(ctx, strategy, numTrees = 1, featureSubsetStrategy = "all", seed = seed)
    val rfModel = rf.train(trainBlock, validBlock)
    rfModel.asInstanceOf[RandomForestModel].trees(0)
  }
}

object DecisionTree {

  /**
    * Method to train a decision tree model.
    * The method supports binary and multiclass classification and regression.
    *
    * @param trainBlock Training dataset: DataBlock of [[LabeledData]].
    *              For classification, labels should take values {0, 1, ..., numClasses-1}.
    *              For regression, labels are real numbers.
    * @param strategy The configuration parameters for the tree algorithm which specify the type
    *                 of decision tree (classification or regression), feature type (continuous,
    *                 categorical), depth of the tree, quantile calculation strategy, etc.
    * @return DecisionTreeModel that can be used for prediction.
    *
    * @note Using `org.apache.spark.mllib.tree.DecisionTree.trainClassifier`
    * and `org.apache.spark.mllib.tree.DecisionTree.trainRegressor`
    * is recommended to clearly separate classification and regression.
    */
  def train(
             ctx: TaskContext,
             trainBlock: DataBlock[LabeledData],
             validBlock: DataBlock[LabeledData],
             strategy: Strategy): MLModel = {
    new DecisionTreeLearner(ctx, strategy).train(trainBlock, validBlock)
  }

  /**
    * Method to train a decision tree model.
    * The method supports binary and multiclass classification and regression.
    *
    * @param trainBlock Training dataset: DataBlock of [[LabeledData]].
    *              For classification, labels should take values {0, 1, ..., numClasses-1}.
    *              For regression, labels are real numbers.
    * @param algo Type of decision tree, either classification or regression.
    * @param impurity Criterion used for information gain calculation.
    * @param maxDepth Maximum depth of the tree (e.g. depth 0 means 1 leaf node, depth 1 means
    *                 1 internal node + 2 leaf nodes).
    * @return DecisionTreeModel that can be used for prediction.
    *
    * @note Using `org.apache.spark.mllib.tree.DecisionTree.trainClassifier`
    * and `org.apache.spark.mllib.tree.DecisionTree.trainRegressor`
    * is recommended to clearly separate classification and regression.
    */
  def train(
             ctx: TaskContext,
             trainBlock: DataBlock[LabeledData],
             validBlock: DataBlock[LabeledData],
             algo: Algo,
             impurity: Impurity,
             maxDepth: Int): MLModel = {
    val strategy = new Strategy(algo, impurity, maxDepth)
    new DecisionTreeLearner(ctx, strategy).train(trainBlock, validBlock)
  }

  /**
    * Method to train a decision tree model.
    * The method supports binary and multiclass classification and regression.
    *
    * @param trainBlock Training dataset: DataBlock of [[LabeledData]].
    *              For classification, labels should take values {0, 1, ..., numClasses-1}.
    *              For regression, labels are real numbers.
    * @param algo Type of decision tree, either classification or regression.
    * @param impurity Criterion used for information gain calculation.
    * @param maxDepth Maximum depth of the tree (e.g. depth 0 means 1 leaf node, depth 1 means
    *                 1 internal node + 2 leaf nodes).
    * @param numClasses Number of classes for classification. Default value of 2.
    * @return DecisionTreeModel that can be used for prediction.
    *
    * @note Using `org.apache.spark.mllib.tree.DecisionTree.trainClassifier`
    * and `org.apache.spark.mllib.tree.DecisionTree.trainRegressor`
    * is recommended to clearly separate classification and regression.
    */
  def train(
             ctx: TaskContext,
             trainBlock: DataBlock[LabeledData],
             validBlock: DataBlock[LabeledData],
             algo: Algo,
             impurity: Impurity,
             maxDepth: Int,
             numClasses: Int): MLModel = {
    val strategy = new Strategy(algo, impurity, maxDepth, numClasses)
    new DecisionTreeLearner(ctx, strategy).train(trainBlock, validBlock)
  }

  /**
    * Method to train a decision tree model.
    * The method supports binary and multiclass classification and regression.
    *
    * @param trainBlock Training dataset: DataBlock of [[LabeledData]].
    *              For classification, labels should take values {0, 1, ..., numClasses-1}.
    *              For regression, labels are real numbers.
    * @param algo Type of decision tree, either classification or regression.
    * @param impurity Criterion used for information gain calculation.
    * @param maxDepth Maximum depth of the tree (e.g. depth 0 means 1 leaf node, depth 1 means
    *                 1 internal node + 2 leaf nodes).
    * @param numClasses Number of classes for classification. Default value of 2.
    * @param maxBins Maximum number of bins used for splitting features.
    * @param quantileCalculationStrategy  Algorithm for calculating quantiles.
    * @param categoricalFeaturesInfo Map storing arity of categorical features. An entry (n to k)
    *                                indicates that feature n is categorical with k categories
    *                                indexed from 0: {0, 1, ..., k-1}.
    * @return DecisionTreeModel that can be used for prediction.
    *
    * @note Using `org.apache.spark.mllib.tree.DecisionTree.trainClassifier`
    * and `org.apache.spark.mllib.tree.DecisionTree.trainRegressor`
    * is recommended to clearly separate classification and regression.
    */
  def train(
             ctx: TaskContext,
             trainBlock: DataBlock[LabeledData],
             validBlock: DataBlock[LabeledData],
             algo: Algo,
             impurity: Impurity,
             maxDepth: Int,
             numClasses: Int,
             maxBins: Int,
             quantileCalculationStrategy: QuantileStrategy,
             categoricalFeaturesInfo: Map[Int, Int]): MLModel = {
    val strategy = new Strategy(algo, impurity, maxDepth, numClasses, maxBins,
      quantileCalculationStrategy, categoricalFeaturesInfo)
    new DecisionTreeLearner(ctx, strategy).train(trainBlock, validBlock)
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
    * @param impurity Criterion used for information gain calculation.
    *                 Supported values: "gini" (recommended) or "entropy".
    * @param maxDepth Maximum depth of the tree (e.g. depth 0 means 1 leaf node, depth 1 means
    *                 1 internal node + 2 leaf nodes).
    *                 (suggested value: 5)
    * @param maxBins Maximum number of bins used for splitting features.
    *                (suggested value: 32)
    * @return DecisionTreeModel that can be used for prediction.
    */
  def trainClassifier(
                       ctx: TaskContext,
                       trainBlock: DataBlock[LabeledData],
                       validBlock: DataBlock[LabeledData],
                       numClasses: Int,
                       categoricalFeaturesInfo: Map[Int, Int],
                       impurity: String,
                       maxDepth: Int,
                       maxBins: Int): MLModel = {
    val impurityType = Impurities.fromString(impurity)
    train(ctx, trainBlock, validBlock, Classification, impurityType, maxDepth, numClasses, maxBins, Sort,
      categoricalFeaturesInfo)
  }

  /**
    * Method to train a decision tree model for regression.
    *
    * @param trainBlock Training dataset: DataBlock of [[LabeledData]].
    *              Labels are real numbers.
    * @param categoricalFeaturesInfo Map storing arity of categorical features. An entry (n to k)
    *                                indicates that feature n is categorical with k categories
    *                                indexed from 0: {0, 1, ..., k-1}.
    * @param impurity Criterion used for information gain calculation.
    *                 The only supported value for regression is "variance".
    * @param maxDepth Maximum depth of the tree (e.g. depth 0 means 1 leaf node, depth 1 means
    *                 1 internal node + 2 leaf nodes).
    *                 (suggested value: 5)
    * @param maxBins Maximum number of bins used for splitting features.
    *                (suggested value: 32)
    * @return DecisionTreeModel that can be used for prediction.
    */
  def trainRegressor(
                      ctx: TaskContext,
                      trainBlock: DataBlock[LabeledData],
                      validBlock: DataBlock[LabeledData],
                      categoricalFeaturesInfo: Map[Int, Int],
                      impurity: String,
                      maxDepth: Int,
                      maxBins: Int): MLModel = {
    val impurityType = Impurities.fromString(impurity)
    train(ctx, trainBlock, validBlock, Regression, impurityType, maxDepth,
      0, maxBins, Sort, categoricalFeaturesInfo)
  }
}

