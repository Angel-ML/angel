package com.tencent.angel.ml.tree.impurity

/**
  * Impurity statistics for each split
  *
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


