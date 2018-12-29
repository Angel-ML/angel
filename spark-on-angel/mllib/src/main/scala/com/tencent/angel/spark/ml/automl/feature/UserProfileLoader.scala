package com.tencent.angel.spark.ml.automl.feature

import com.tencent.angel.spark.ml.automl.feature.preprocess.{SamplerWrapper, StopWordsRemoverWrapper, TokenizerWrapper}

import scala.collection.mutable.ArrayBuffer

class UserProfileLoader {

  private var selectedComponents: ArrayBuffer[String] = ???

  private def componentToTransformers(component: String): TransformerWrapper = {
    component match {
      case "SamplerWrapper" => new SamplerWrapper(0.5)
      case "StopWordsRemoverWrapper" => new StopWordsRemoverWrapper()
      case "Tokenizer" => new TokenizerWrapper()
    }
  }

  def load(): Array[TransformerWrapper] = {
    _
  }
}
