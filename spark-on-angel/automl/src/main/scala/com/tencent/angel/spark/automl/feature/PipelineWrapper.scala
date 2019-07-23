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

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.{DataFrame, Dataset}

class PipelineWrapper() {

  var pipeline = new Pipeline()

  var transformers: Array[TransformerWrapper] = Array()

  def setTransformers(value: Array[TransformerWrapper]): this.type = {
    transformers = value
    setStages(PipelineBuilder.build(transformers))
    this
  }

  def setStages(value: Array[_ <: PipelineStage]): Unit = {
    pipeline = pipeline.setStages(value)
  }

  def fit(dataset: Dataset[_]): PipelineModelWrapper = {
    new PipelineModelWrapper(pipeline.fit(dataset), transformers)
  }

}

class PipelineModelWrapper(val model: PipelineModel,
                           val transformers: Array[TransformerWrapper]) {

  def transform(dataset: Dataset[_]): DataFrame = {
    var df = model.transform(dataset)
    if (transformers.length >= 2) {
      (0 until transformers.length - 1).foreach{ i =>
        val outCols = transformers(i).getOutputCols
        for (col <- outCols) {
          df = df.drop(col)
        }
      }
    }
    df
  }
}
