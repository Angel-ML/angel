package com.tencent.angel.ml.tree

import java.util.Locale

import com.tencent.angel.ml.tree.conf.{Algo => OldAlgo, BoostingStrategy => OldBoostingStrategy, Strategy => OldStrategy}
import com.tencent.angel.ml.tree.impurity.{Entropy => OldEntropy, Gini => OldGini, Impurity => OldImpurity, Variance => OldVariance}
import com.tencent.angel.ml.tree.loss.{AbsoluteError => OldAbsoluteError, ClassificationLoss => OldClassificationLoss, LogLoss => OldLogLoss, Loss => OldLoss, SquaredError => OldSquaredError}

/**
  * Parameters for Decision Tree-based algorithms.
  *
  * Note: Marked as private and DeveloperApi since this may be made public in the future.
  */
trait DecisionTreeParams {

  /**
    * Maximum depth of the tree (>= 0).
    * E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
    * (default = 5)
    * @group param
    */
  final var maxDepth: Int = 5
  final val maxDepthInfo = "maxDepth: Maximum depth of the tree. (>= 0)" +
    " E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes."

  /**
    * Maximum number of bins used for discretizing continuous features and for choosing how to split
    * on features at each node.  More bins give higher granularity.
    * Must be >= 2 and >= number of categories in any categorical feature.
    * (default = 32)
    * @group param
    */
  final var maxBins: Int = 32
  final val maxBinsInfo = "maxBins: Max number of bins for discretizing continuous features." +
    " Must be >=2 and >= number of categories for any categorical feature."

  /**
    * Minimum number of instances each child must have after split.
    * If a split causes the left or right child to have fewer than minInstancesPerNode,
    * the split will be discarded as invalid.
    * Should be >= 1.
    * (default = 1)
    * @group param
    */
  final var minInstancesPerNode: Int = 1
  final val minInstancesPerNodeInfo = "minInstancesPerNode: Minimum number of instances each child must have after split." +
    " If a split causes the left or right child to have fewer than minInstancesPerNode," +
    " the split will be discarded as invalid. Should be >= 1."

  /**
    * Minimum information gain for a split to be considered at a tree node.
    * Should be >= 0.0.
    * (default = 0.0)
    * @group param
    */
  final var minInfoGain: Double = 0.0
  final val minInfoGainInfo = "minInfoGain: Minimum information gain for a split to be considered at a tree node."

  /**
    * Maximum memory in MB allocated to histogram aggregation. If too small, then 1 node will be
    * split per iteration, and its aggregates may exceed this size.
    * (default = 256 MB)
    * @group expertParam
    */
  final var maxMemoryInMB: Int = 256
  final val maxMemoryInMBInfo = "maxMemoryInMB: Maximum memory in MB allocated to histogram aggregation."

  /**
    * If false, the algorithm will pass trees to executors to match instances with nodes.
    * If true, the algorithm will cache node IDs for each instance.
    * Caching can speed up training of deeper trees. Users can set how often should the
    * cache be checkpointed or disable it by setting checkpointInterval.
    * (default = false)
    * @group expertParam
    */
  final var cacheNodeIds: Boolean = false
  final val cacheNodeIdsInfo = "cacheNodeIds: If false, the algorithm will pass trees to executors to" +
    " match instances with nodes. If true, the algorithm will cache node IDs for each instance. " +
    " Caching can speed up training of deeper trees."


  def setMaxDepth(value: Int): this.type = {
    maxDepth = value
    this
  }

  def getMaxDepth: Int = maxDepth

  def setMaxBins(value: Int): this.type = {
    maxBins = value
    this
  }

  def getMaxBins: Int = maxBins

  def setMinInstancesPerNode(value: Int): this.type = {
    minInstancesPerNode = value
    this
  }

  def getMinInstancesPerNode: Int = minInstancesPerNode

  def setMinInfoGain(value: Double): this.type =
  {
    minInfoGain = value
    this
  }

  def getMinInfoGain: Double = minInfoGain

  def setMaxMemoryInMB(value: Int): this.type = {
    maxMemoryInMB = value
    this
  }

  def getMaxMemoryInMB: Int = maxMemoryInMB

  def setCacheNodeIds(value: Boolean): this.type = {
    cacheNodeIds = value
    this
  }

  final def getCacheNodeIds: Boolean = cacheNodeIds

  def getOldStrategy(
                      categoricalFeatures: Map[Int, Int],
                      numClasses: Int,
                      oldAlgo: OldAlgo.Algo,
                      oldImpurity: OldImpurity,
                      subsamplingRate: Double): OldStrategy = {
    val strategy = OldStrategy.defaultStrategy(oldAlgo)
    strategy.impurity = oldImpurity
    strategy.maxBins = getMaxBins
    strategy.maxDepth = getMaxDepth
    strategy.maxMemoryInMB = getMaxMemoryInMB
    strategy.minInfoGain = getMinInfoGain
    strategy.minInstancesPerNode = getMinInstancesPerNode
    strategy.useNodeIdCache = getCacheNodeIds
    strategy.numClasses = numClasses
    strategy.categoricalFeaturesInfo = categoricalFeatures
    strategy.subsamplingRate = subsamplingRate
    strategy
  }
}

/**
  * Parameters for Decision Tree-based classification algorithms.
  */
trait TreeClassifierParams {

  /**
    * Criterion used for information gain calculation (case-insensitive).
    * Supported: "entropy" and "gini".
    * (default = gini)
    * @group param
    */
  final var impurity = "gini"
  final val impurityInfo = "impurity: Criterion used for information gain calculation (case-insensitive). Supported options:" +
    s" ${TreeClassifierParams.supportedImpurities.mkString(", ")}"

  def setImpurity(value: String): this.type = {
    impurity = value
    this
  }

  def getImpurity: String = impurity.toLowerCase(Locale.ROOT)

  /** Convert new impurity to old impurity. */
  def getOldImpurity: OldImpurity = {
    getImpurity match {
      case "entropy" => OldEntropy
      case "gini" => OldGini
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(
          s"TreeClassifierParams was given unrecognized impurity: $impurity.")
    }
  }
}

object TreeClassifierParams {
  // These options should be lowercase.
  final val supportedImpurities: Array[String] =
    Array("entropy", "gini").map(_.toLowerCase(Locale.ROOT))
}

trait DecisionTreeClassifierParams
  extends DecisionTreeParams with TreeClassifierParams

/**
  * Parameters for Decision Tree-based regression algorithms.
  */
trait TreeRegressorParams {

  /**
    * Criterion used for information gain calculation (case-insensitive).
    * Supported: "variance".
    * (default = variance)
    * @group param
    */
  final var impurity = "variance"
  final val impurityInfo = "impurity: Criterion used for information gain calculation (case-insensitive). Supported options:" +
    s" ${TreeRegressorParams.supportedImpurities.mkString(", ")}"

  def setImpurity(value: String): this.type = {
    impurity = value
    this
  }

  def getImpurity: String = impurity.toLowerCase(Locale.ROOT)

  /** Convert new impurity to old impurity. */
  private[ml] def getOldImpurity: OldImpurity = {
    getImpurity match {
      case "variance" => OldVariance
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(
          s"TreeRegressorParams was given unrecognized impurity: $impurity")
    }
  }
}

object TreeRegressorParams {
  // These options should be lowercase.
  final val supportedImpurities: Array[String] =
    Array("variance").map(_.toLowerCase(Locale.ROOT))
}

trait DecisionTreeRegressorParams
  extends DecisionTreeParams with TreeRegressorParams

object TreeEnsembleParams {
  // These options should be lowercase.
  final val supportedFeatureSubsetStrategies: Array[String] =
    Array("auto", "all", "onethird", "sqrt", "log2").map(_.toLowerCase(Locale.ROOT))
}

/**
  * Parameters for Decision Tree-based ensemble algorithms.
  *
  * Note: Marked as private and DeveloperApi since this may be made public in the future.
  */
trait TreeEnsembleParams extends DecisionTreeParams {

  /**
    * Fraction of the training data used for learning each decision tree, in range (0, 1].
    * (default = 1.0)
    * @group param
    */
  final var subsamplingRate: Double = 1.0
  final val subsamplingRateInfo = "subsamplingRate: Fraction of the training data used for" +
    " learning each decision tree, in range (0, 1]."

  def setSubsamplingRate(value: Double): this.type = {
    subsamplingRate = value
    this
  }

  def getSubsamplingRate: Double = subsamplingRate

  def getOldStrategy(
                      categoricalFeatures: Map[Int, Int],
                      numClasses: Int,
                      oldAlgo: OldAlgo.Algo,
                      oldImpurity: OldImpurity): OldStrategy = {
    super.getOldStrategy(categoricalFeatures, numClasses, oldAlgo, oldImpurity, getSubsamplingRate)
  }

  /**
    * The number of features to consider for splits at each tree node.
    * Supported options:
    *  - "auto": Choose automatically for task:
    *            If numTrees == 1, set to "all."
    *            If numTrees > 1 (forest), set to "sqrt" for classification and
    *              to "onethird" for regression.
    *  - "all": use all features
    *  - "onethird": use 1/3 of the features
    *  - "sqrt": use sqrt(number of features)
    *  - "log2": use log2(number of features)
    *  - "n": when n is in the range (0, 1.0], use n * number of features. When n
    *         is in the range (1, number of features), use n features.
    * (default = "auto")
    *
    * These various settings are based on the following references:
    *  - log2: tested in Breiman (2001)
    *  - sqrt: recommended by Breiman manual for random forests
    *  - The defaults of sqrt (classification) and onethird (regression) match the R randomForest
    *    package.
    * @see <a href="http://www.stat.berkeley.edu/~breiman/randomforest2001.pdf">Breiman (2001)</a>
    * @see <a href="http://www.stat.berkeley.edu/~breiman/Using_random_forests_V3.1.pdf">
    * Breiman manual for random forests</a>
    *
    * @group param
    */
  final var featureSubsetStrategy = "auto"
  final val featureSubsetStrategyInfo = "featureSubsetStrategy: The number of features to consider for splits at each tree node." +
    s" Supported options: ${TreeEnsembleParams.supportedFeatureSubsetStrategies.mkString(", ")},, (0.0-1.0], [1-n]."

  def setFeatureSubsetStrategy(value: String): this.type = {
    featureSubsetStrategy = value
    this
  }

  final def getFeatureSubsetStrategy: String = featureSubsetStrategy.toLowerCase(Locale.ROOT)
}

/**
  * Parameters for Random Forest algorithms.
  */
trait RandomForestParams extends TreeEnsembleParams {

  /**
    * Number of trees to train (>= 1).
    * If 1, then no bootstrapping is used.  If > 1, then bootstrapping is done.
    * TODO: Change to always do bootstrapping (simpler).  SPARK-7130
    * (default = 20)
    *
    * Note: The reason that we cannot add this to both GBT and RF (i.e. in TreeEnsembleParams)
    * is the param `maxIter` controls how many trees a GBT has. The semantics in the algorithms
    * are a bit different.
    * @group param
    */
  final var numTrees: Int = 20
  final val numTreesInfo = "numTrees: Number of trees to train (>= 1)."

  def setNumTrees(value: Int): this.type = {
    numTrees = value
    this
  }

  def getNumTrees: Int = numTrees
}

trait RandomForestClassifierParams
  extends RandomForestParams with TreeClassifierParams

trait RandomForestRegressorParams
  extends RandomForestParams with TreeRegressorParams

/**
  * Parameters for Gradient-Boosted Tree algorithms.
  *
  * Note: Marked as private and DeveloperApi since this may be made public in the future.
  */
trait GBTParams extends TreeEnsembleParams {

  /**
    * Threshold for stopping early when fit with validation is used.
    * (This parameter is ignored when fit without validation is used.)
    * The decision to stop early is decided based on this logic:
    * If the current loss on the validation set is greater than 0.01, the diff
    * of validation error is compared to relative tolerance which is
    * validationTol * (current loss on the validation set).
    * If the current loss on the validation set is less than or equal to 0.01,
    * the diff of validation error is compared to absolute tolerance which is validationTol * 0.01.
    * @group param
    * @see validationIndicatorCol
    */
  final var validationTol: Double = 0.01
  final val validationTolInfo = "validationTol: Threshold for stopping early when fit with validation is used." +
      " If the error rate on the validation input changes by less than the validationTol," +
      " then learning will stop early (before `maxIter`). This parameter is ignored when fit without validation is used."

  def setValidationTol(value: Double): this.type = {
    validationTol = value
    this
  }

  def getValidationTol: Double = validationTol

  /**
    * Param for maximum number of iterations (&gt;= 0).
    * @group param
    */
  final var maxIter: Int = 20
  final val maxIterInfo = "maxIter: maximum number of iterations (>= 0)"

  def getMaxIter: Int = maxIter

  def setMaxIter(value: Int): this.type = {
    maxIter = value
    this
  }

  /**
    * Param for Step size (a.k.a. learning rate) in interval (0, 1] for shrinking
    * the contribution of each estimator.
    * (default = 0.1)
    * @group param
    */
  final var stepSize: Double = 0.1
  final val stepSizeInfo = "stepSize: Step size (a.k.a. learning rate) in interval (0, 1] for" +
    " shrinking the contribution of each estimator."

  def getStepSize: Double = stepSize

  def setStepSize(value: Double): this.type = {
    stepSize = value
    this
  }

  setFeatureSubsetStrategy("all")

  /** (private[ml]) Create a BoostingStrategy instance to use with the old API. */
  def getOldBoostingStrategy(
                              categoricalFeatures: Map[Int, Int],
                              oldAlgo: OldAlgo.Algo): OldBoostingStrategy = {
    val strategy = super.getOldStrategy(categoricalFeatures, numClasses = 2, oldAlgo, OldVariance)
    // NOTE: The old API does not support "seed" so we ignore it.
    new OldBoostingStrategy(strategy, getOldLossType, getMaxIter, getStepSize, getValidationTol)
  }

  /** Get old Gradient Boosting Loss type */
  def getOldLossType: OldLoss
}

object GBTClassifierParams {
  // The losses below should be lowercase.
  /** Accessor for supported loss settings: logistic */
  final val supportedLossTypes: Array[String] =
    Array("logistic").map(_.toLowerCase(Locale.ROOT))
}

trait GBTClassifierParams extends GBTParams with TreeClassifierParams {

  /**
    * Loss function which GBT tries to minimize. (case-insensitive)
    * Supported: "logistic"
    * (default = logistic)
    * @group param
    */
  var lossType = "logistic"
  val lossTypeInfo = "lossType: Loss function which GBT tries to minimize (case-insensitive). Supported options:" +
    s" ${GBTClassifierParams.supportedLossTypes.mkString(", ")}"

  def getLossType: String = lossType.toLowerCase(Locale.ROOT)

  /** (private[ml]) Convert new loss to old loss. */
  def getOldLossType: OldClassificationLoss = {
    getLossType match {
      case "logistic" => OldLogLoss
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(s"GBTClassifier was given bad loss type: $getLossType")
    }
  }
}

object GBTRegressorParams {
  // The losses below should be lowercase.
  /** Accessor for supported loss settings: squared (L2), absolute (L1) */
  final val supportedLossTypes: Array[String] =
    Array("squared", "absolute").map(_.toLowerCase(Locale.ROOT))
}

trait GBTRegressorParams extends GBTParams with TreeRegressorParams {

  /**
    * Loss function which GBT tries to minimize. (case-insensitive)
    * Supported: "squared" (L2) and "absolute" (L1)
    * (default = squared)
    * @group param
    */
  var lossType = "squared"
  val lossTypeInfo = "lossType: Loss function which GBT tries to minimize (case-insensitive). Supported options:" +
    s" ${GBTRegressorParams.supportedLossTypes.mkString(", ")}"

  def getLossType: String = lossType.toLowerCase(Locale.ROOT)

  /** (private[ml]) Convert new loss to old loss. */
  def getOldLossType: OldLoss = {
    convertToOldLossType(getLossType)
  }

  def convertToOldLossType(loss: String): OldLoss = {
    loss match {
      case "squared" => OldSquaredError
      case "absolute" => OldAbsoluteError
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(s"GBTRegressorParams was given bad loss type: $getLossType")
    }
  }
}

