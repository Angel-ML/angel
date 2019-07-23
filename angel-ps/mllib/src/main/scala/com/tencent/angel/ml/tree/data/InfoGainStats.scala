package com.tencent.angel.ml.tree.data

import com.tencent.angel.ml.tree.oldmodel.Predict

/**
  * Information gain statistics for each split
  * @param gain information gain value
  * @param impurity current node impurity
  * @param leftImpurity left node impurity
  * @param rightImpurity right node impurity
  * @param leftPredict left node predict
  * @param rightPredict right node predict
  */
class InfoGainStats(
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
    case other: InfoGainStats =>
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