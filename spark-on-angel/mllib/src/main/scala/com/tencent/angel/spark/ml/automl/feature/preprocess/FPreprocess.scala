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


package com.tencent.angel.spark.ml.automl.feature.preprocess

import com.tencent.angel.ml.core.conf.MLConf
import com.tencent.angel.spark.ml.automl.AutoConf
import com.tencent.angel.spark.ml.automl.feature.DataLoader
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer


object FPreprocess {

  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val master = params.getOrElse("master", "yarn")
    val deploy = params.getOrElse("deploy-mode", "cluster")
    val input = params.getOrElse("input", "")
    val inputSeparator = params.getOrElse(MLConf.ML_DATA_SPLITOR,
      MLConf.DEFAULT_ML_DATA_SPLITOR)
    val inputFormat = params.getOrElse(MLConf.ML_DATA_INPUT_FORMAT,
      MLConf.DEFAULT_ML_DATA_INPUT_FORMAT)
    val inputType = params.getOrElse(AutoConf.Preprocess.INPUT_TYPE,
      AutoConf.Preprocess.DEFAULT_INPUT_TYPE)
    val sampleRate = params.getOrElse(AutoConf.Preprocess.SAMPLE_RATE,
      AutoConf.Preprocess.DEFAULT_SAMPLE_RATE
    )
    val imbalanceSampleRate = params.getOrElse(AutoConf.Preprocess.IMBALANCE_SAMPLE,
      AutoConf.Preprocess.DEFAULT_IMBALANCE_SAMPLE)
    val hasTokenizer = if (inputFormat.equals("document")) true else false
    val hasStopWordsRemover = if (inputFormat.equals("document")) true else false

    val ss = SparkSession
      .builder
      .master(master + "-" + deploy)
      .appName("preprocess")
      .getOrCreate()

    val training = DataLoader.load(ss, inputFormat, input, inputSeparator)

    var components = new ArrayBuffer[PipelineStage]

    if (hasTokenizer)
      Components.addTokenizer(components,
        "sentence", "words")

    if (hasTokenizer)
      Components.addTokenizer(components,
        "words", "filterWords")


    val pipeline = new Pipeline()
      .setStages(components.toArray)

    val model = pipeline.fit(training)

    ss.stop()

  }

}
