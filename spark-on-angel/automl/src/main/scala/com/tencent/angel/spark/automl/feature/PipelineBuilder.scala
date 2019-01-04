/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.spark.automl.feature

import org.apache.spark.SparkException
import org.apache.spark.ml.PipelineStage

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class IncompatibleFiledExecption(msg: String) extends SparkException(msg) { }

object PipelineBuilder {

  def build(transformers: Array[TransformerWrapper]): Array[PipelineStage] = {
    val stages: ArrayBuffer[PipelineStage] = new ArrayBuffer[PipelineStage]()
    //val allInputCols: ArrayBuffer[String] = new ArrayBuffer[String]()
    val allInputCols: mutable.HashSet[String] = new mutable.HashSet[String]()

    (1 to transformers.length).foreach { i =>
      // set parent
      transformers(i).setParent(transformers(i - 1))
      // add new cols
      allInputCols ++= transformers(i - 1).getOutputCols
      // set parent cols
      transformers(i).setAncestorCols(allInputCols.toArray)
      // generate input cols
      transformers(i).generateInputCols()
      // generate output cols
      transformers(i).generateOutputCols()
      // add fully configured transformer
      stages += transformers(i).declareInAndOut().getTransformer
    }

    stages.toArray
  }

}
