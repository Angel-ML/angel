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


package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.RunningMode
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.{ArgsUtil, GraphModel, OfflineLearner}
import org.apache.spark.{SparkConf, SparkContext}

object OfflineRunner {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)

    // Train data path
    val input = params.getOrElse("input", "")

    // Can be used in train/predict mode, it means model output path in train mode and model load path in predict mode
    val output = params.getOrElse("modelPath", "")
    val actionType = params.getOrElse("actionType", "train")
    val network = params.getOrElse("network", "LogisticRegression")

    // Model load path in train mode, just use in train mode
    val modelPath = params.getOrElse("model", "")

    // Predict result save path, just use in predict mode
    val predictPath = params.getOrElse("predictPath", "")

    // set running mode, use angel_ps mode for spark
    SharedConf.get().set(AngelConf.ANGEL_RUNNING_MODE, RunningMode.ANGEL_PS.toString)

    // build SharedConf with params
    SharedConf.addMap(params)

    val dim = SharedConf.indexRange.toInt

    println(s"dim=$dim")

    // load data
    val conf = new SparkConf()

    // we set the load model path for angel-ps to load the meta information of model
    actionType match {
      case MLConf.ANGEL_ML_TRAIN => {
        if(modelPath.length > 0)
          conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, modelPath)
      }
      case MLConf.ANGEL_ML_PREDICT => {
        if(output.length > 0)
          conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, output)
      }
    }

    val sc   = new SparkContext(conf)

    // start PS
    PSContext.getOrCreate(sc)

    val className = "com.tencent.angel.spark.ml.classification." + network
    val model = GraphModel(className)
    val learner = new OfflineLearner

    actionType match {
      case MLConf.ANGEL_ML_TRAIN => learner.train(input, output, modelPath, dim, model)
      case MLConf.ANGEL_ML_PREDICT => learner.predict(input, predictPath, output, dim, model)
    }
  }
}