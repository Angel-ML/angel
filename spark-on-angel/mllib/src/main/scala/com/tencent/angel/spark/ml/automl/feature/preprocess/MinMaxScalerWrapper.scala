package com.tencent.angel.spark.ml.automl.feature.preprocess

import com.tencent.angel.spark.ml.automl.feature.InToOutRelation.{InToOutRelation, OneToOne}
import com.tencent.angel.spark.ml.automl.feature.TransformerWrapper
import org.apache.spark.ml.{Estimator, Transformer}
import org.apache.spark.ml.feature.{MinMaxScaler, MinMaxScalerModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

private[preprocess] class MinMaxScalerWrapper extends TransformerWrapper {

  override var transformer: Transformer  = _
  val estimator = new MinMaxScaler()
  override val parent: TransformerWrapper = _

  override val hasMultiInputs: Boolean = false
  override val hasMultiOutputs: Boolean = false
  override val needAncestorInputs: Boolean = false

  override val relation: InToOutRelation = OneToOne

  override val requiredInputCols: Array[String] = Array("numerical")
  override val requiredOutputCols: Array[String] = Array("minMaxNumerical")

  override def declareInAndOut(): this.type = {
    transformer.asInstanceOf[MinMaxScalerModel].setInputCol(getInputCols(0))
    transformer.asInstanceOf[MinMaxScalerModel].setOutputCol(getOutputCols(0))
    this
  }

  def fit(dataFrame: DataFrame): Unit = {
    transformer = estimator.fit(dataFrame)
  }

  def transform(dataset: Dataset[_]): DataFrame = {

    val inputCol = "features"
    val OutputCol = "scaledFeatures"

    val df = dataset.toDF()

    val scaler = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")

    val scalerModel = scaler.fit(df)

    val scaledDf = scalerModel.transform(df)

    scaledDf.drop("features").withColumnRenamed("scaledFeatures", "features")
  }
}
