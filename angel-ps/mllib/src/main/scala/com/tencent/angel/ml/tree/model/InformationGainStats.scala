package com.tencent.angel.ml.tree.model

import com.tencent.angel.ml.tree.impurity.ImpurityCalculator

/**
  * Information gain statistics for each split
  * @param gain information gain value
  * @param impurity current node impurity
  * @param leftImpurity left node impurity
  * @param rightImpurity right node impurity
  * @param leftPredict left node predict
  * @param rightPredict right node predict
  */
class InformationGainStats(
                            val gain: Float,
                            val impurity: Float,
                            val leftImpurity: Float,
                            val rightImpurity: Float,
                            val leftPredict: Predict,
                            val rightPredict: Predict) extends Serializable {

  override def toString: String = {
    s"gain = $gain, impurity = $impurity, left impurity = $leftImpurity, " +
      s"right impurity = $rightImpurity"
  }

  override def equals(o: Any): Boolean = o match {
    case other: InformationGainStats =>
      gain == other.gain &&
        impurity == other.impurity &&
        leftImpurity == other.leftImpurity &&
        rightImpurity == other.rightImpurity &&
        leftPredict == other.leftPredict &&
        rightPredict == other.rightPredict

    case _ => false
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(
      gain: java.lang.Float,
      impurity: java.lang.Float,
      leftImpurity: java.lang.Float,
      rightImpurity: java.lang.Float,
      leftPredict,
      rightPredict)
  }
}

/**
  * Impurity statistics for each split
  * @param gain information gain value
  * @param impurity current node impurity
  * @param impurityCalculator impurity statistics for current node
  * @param leftImpurityCalculator impurity statistics for left child node
  * @param rightImpurityCalculator impurity statistics for right child node
  * @param valid whether the current split satisfies minimum info gain or
  *              minimum number of instances per node
  */
private[tree] class ImpurityStats(
                                    val gain: Float,
                                    val impurity: Float,
                                    val impurityCalculator: ImpurityCalculator,
                                    val leftImpurityCalculator: ImpurityCalculator,
                                    val rightImpurityCalculator: ImpurityCalculator,
                                    val valid: Boolean = true) extends Serializable {

  override def toString: String = {
    s"gain = $gain, impurity = $impurity, left impurity = $leftImpurity, " +
      s"right impurity = $rightImpurity"
  }

  def leftImpurity: Float = if (leftImpurityCalculator != null) {
    leftImpurityCalculator.calculate()
  } else {
    -1.0f
  }

  def rightImpurity: Float = if (rightImpurityCalculator != null) {
    rightImpurityCalculator.calculate()
  } else {
    -1.0f
  }
}

private[tree] object ImpurityStats {

  /**
    * Return an [[org.apache.spark.mllib.tree.model.ImpurityStats]] object to
    * denote that current split doesn't satisfies minimum info gain or
    * minimum number of instances per node.
    */
  def getInvalidImpurityStats(impurityCalculator: ImpurityCalculator): ImpurityStats = {
    new ImpurityStats(Float.MinValue, impurityCalculator.calculate(),
      impurityCalculator, null, null, false)
  }

  /**
    * Return an [[org.apache.spark.mllib.tree.model.ImpurityStats]] object
    * that only 'impurity' and 'impurityCalculator' are defined.
    */
  def getEmptyImpurityStats(impurityCalculator: ImpurityCalculator): ImpurityStats = {
    new ImpurityStats(Float.NaN, impurityCalculator.calculate(), impurityCalculator, null, null)
  }
}

