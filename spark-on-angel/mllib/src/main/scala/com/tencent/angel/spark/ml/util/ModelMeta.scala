package com.tencent.angel.spark.ml.util

import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, RegressionMetrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

// the model is for train model and model-meta
class ModelMeta(val modelSummary: Model) {

  def this() = this(null)

  // set model meta for classification
  def setMulClaModelMeta(sc: SparkContext,
                         input: String,
                         partitionNum: Int,
                         sampleRate: Double,
                         featureColExpr: String,
                         labelCol: Int,
                         prefix: String = "",
                         sep: String = " ") = {

    println("classification model meta")
    val predictionAndLabels = getPreAndLabel(modelSummary, sc, input, partitionNum, sampleRate, featureColExpr, labelCol, sep)
    val metrics = new MulticlassMetrics(predictionAndLabels)

    val f1 = metrics.weightedFMeasure
    val precision = metrics.accuracy
    val recall = metrics.accuracy
    val weiPrecision = metrics.weightedPrecision
    val weiRecall = metrics.weightedRecall

    modelSummary.modelMeta.logMeta(prefix + "f1", f1)
    modelSummary.modelMeta.logMeta(prefix + "precision", precision)
    modelSummary.modelMeta.logMeta(prefix + "recall", recall)
    modelSummary.modelMeta.logMeta(prefix + "weighted_precision", weiPrecision)
    modelSummary.modelMeta.logMeta(prefix + "weighted_recall", weiRecall)
  }

  // set model meta for regression
  def setRegModelMeta(sc: SparkContext,
                      input: String,
                      partitionNum: Int,
                      sampleRate: Double,
                      featureColExpr: String,
                      labelCol: Int,
                      prefix: String = "",
                      sep: String = " ") = {

    println("regression model meta")
    val predictionAndLabels = getPreAndLabel(modelSummary, sc, input, partitionNum, sampleRate, featureColExpr, labelCol, sep)
    val metrics = new RegressionMetrics(predictionAndLabels)

    val mse = metrics.meanSquaredError
    val rmse = metrics.rootMeanSquaredError
    val r2 = metrics.r2
    val mae = metrics.meanAbsoluteError
    val expVariance = metrics.explainedVariance

    modelSummary.modelMeta.logMeta(prefix + "MSE", mse)
    modelSummary.modelMeta.logMeta(prefix + "RMSE", rmse)
    modelSummary.modelMeta.logMeta(prefix + "R_squared", r2)
    modelSummary.modelMeta.logMeta(prefix + "MAE", mae)
    modelSummary.modelMeta.logMeta(prefix + "explained_variance", expVariance)
  }

  def getPreAndLabel(model: Model,
                     sc: SparkContext,
                     input: String,
                     partitionNum: Int,
                     sampleRate: Double,
                     featureColExpr: String,
                     labelCol: Int,
                     sep: String = " "): RDD[(Double, Double)] = {
    val inputDF = DataLoader.loadlabeled(sc, input, partitionNum, sampleRate, featureColExpr, labelCol, false, sep)

    val predictionAndLabels = model.predict(inputDF)
      .select(DFStruct.PROB, DFStruct.LABEL).rdd
      .map { case Row(prediction: Double, label: Double) =>
        (prediction, label)
      }

    predictionAndLabels
  }

}