package com.tencent.angel.spark.automl.feature

import com.tencent.angel.spark.automl.feature.preprocess._

import scala.collection.mutable.ArrayBuffer

class UserProfileLoader {

  private var selectedComponents: ArrayBuffer[String] = ???

  private def componentToTransformers(component: String): TransformerWrapper = {
    component match {
      case "SamplerWrapper" => new SamplerWrapper(0.5)
      case "StopWordsRemoverWrapper" => new StopWordsRemoverWrapper()
      case "Tokenizer" => new TokenizerWrapper()
      case "MinMaxScalerWrapper" => new MinMaxScalerWrapper()
      case "StandardScalerWrapper" => new StandardScalerWrapper()
    }
  }

}
