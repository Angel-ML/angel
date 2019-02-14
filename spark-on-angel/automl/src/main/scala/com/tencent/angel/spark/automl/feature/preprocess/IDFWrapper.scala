package com.tencent.angel.spark.automl.feature.preprocess

import com.tencent.angel.spark.automl.feature.InToOutRelation.{InToOutRelation, OneToOne}
import com.tencent.angel.spark.automl.feature.TransformerWrapper
import org.apache.spark.ml.feature.{IDF, StandardScaler}

class IDFWrapper extends TransformerWrapper {

  override val transformer = new IDF()
  override var parent: TransformerWrapper = _

  override val hasMultiInputs: Boolean = false
  override val hasMultiOutputs: Boolean = false
  override val needAncestorInputs: Boolean = false

  override val relation: InToOutRelation = OneToOne

  override val requiredInputCols: Array[String] = Array("rawFeatures")
  override val requiredOutputCols: Array[String] = Array("outIDF")

  override def declareInAndOut(): this.type = {
    transformer.asInstanceOf[IDF].setInputCol(getInputCols(0))
    transformer.asInstanceOf[IDF].setOutputCol(getOutputCols(0))
    this
  }
}
