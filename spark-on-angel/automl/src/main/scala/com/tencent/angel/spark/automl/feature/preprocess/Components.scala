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


package com.tencent.angel.spark.automl.feature.preprocess

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

object Components {

  def sample(data: DataFrame,
             fraction: Double): DataFrame = {
    data.sample(false, fraction)
  }

  def addSampler(components: ArrayBuffer[PipelineStage],
                 inputCol: String,
                 fraction: Double): Unit = {
    val sampler = new Sampler(fraction)
      .setInputCol("features")
    components += sampler
  }

  def addTokenizer(components: ArrayBuffer[PipelineStage],
                   inputCol: String,
                   outputCol: String): Unit = {
    val tokenizer = new Tokenizer()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
    components += tokenizer
  }

  def addStopWordsRemover(components: ArrayBuffer[PipelineStage],
                          inputCol: String,
                          outputCol: String): Unit = {
    val remover = new StopWordsRemover()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
    components += remover
  }

}
