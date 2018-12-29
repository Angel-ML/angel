package com.tencent.angel.spark.ml.automl.feature

import org.apache.spark.ml.Pipeline

class PipelineWrapper {

  var components: Array[TransformerWrapper] = ???

  val pipeline = new Pipeline()

  def setComponents(components: Array[TransformerWrapper]): Unit = {
    this.components = components
  }

  def fit() = _

  def transform() = _

}

class LinearPipeline {

}

class DAGPipeline {

}
