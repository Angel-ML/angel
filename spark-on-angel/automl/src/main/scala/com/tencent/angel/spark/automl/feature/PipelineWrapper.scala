package com.tencent.angel.spark.automl.feature

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.Dataset

class PipelineWrapper() {

  var pipeline = new Pipeline()

  def setStages(value: Array[_ <: PipelineStage]): Unit = {
    pipeline = pipeline.setStages(value)
  }

  def fit(dataset: Dataset[_]): PipelineModel = {
    pipeline.fit(dataset)
  }

}
