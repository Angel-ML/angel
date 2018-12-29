package com.tencent.angel.spark.ml.automl.feature.preprocess

import com.tencent.angel.spark.ml.automl.feature.InToOutRelation.{InToOutRelation, OneToOne}
import com.tencent.angel.spark.ml.automl.feature.TransformerWrapper
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.ml.feature.{StandardScaler, StandardScalerModel}

class StandardScalerWrapper extends TransformerWrapper {

  override val transformer: Transformer = _
  override val parent: TransformerWrapper = _
  val estimator = new StandardScaler()

  override val hasMultiInputs: Boolean = false
  override val hasMultiOutputs: Boolean = false
  override val needAncestorInputs: Boolean = false

  override val relation: InToOutRelation = OneToOne

  override val requiredInputCols: Array[String] = Array("numerical")
  override val requiredOutputCols: Array[String] = Array("standardNumerical")

  override def declareInAndOut(): this.type = {
    transformer.asInstanceOf[StandardScalerModel].setInputCol(getInputCols(0))
    transformer.asInstanceOf[StandardScalerModel].setOutputCol(getOutputCols(0))
    this
  }

  def fit(df: DataFrame): Transformer = {
    estimator.fit(df)
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    val df = dataset.toDF()

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(true)
    val scalerModel = scaler.fit(df)

    val scaledDf = scalerModel.transform(df)

    scaledDf.drop("features").withColumnRenamed("scaledFeatures", "features")
  }
}
