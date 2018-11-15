package com.tencent.angel.ml.tree.oldmodel

/**
  * Predicted value for a node
  * @param predict predicted value
  * @param prob probability of the label (classification only)
  */
class Predict (
                val predict: Float,
                val prob: Float = 0.0f) extends Serializable {

  override def toString: String = s"$predict (prob = $prob)"

  override def equals(other: Any): Boolean = {
    other match {
      case p: Predict => predict == p.predict && prob == p.prob
      case _ => false
    }
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(predict: java.lang.Float, prob: java.lang.Float)
  }
}

