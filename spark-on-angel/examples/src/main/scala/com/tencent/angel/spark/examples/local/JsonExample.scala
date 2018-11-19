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


package com.tencent.angel.spark.examples.local

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.utils.paramsutils.JsonUtils
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.{ArgsUtil, GraphModel, OfflineLearner}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}

object JsonExample {

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("conf/log4j.properties")
    val params = ArgsUtil.parse(args)
    val input = params.getOrElse("input", "data/census/census_148d_train.libsvm")
    val output = params.getOrElse("output", "")
    val modelPath = params.getOrElse("model", "")
    val actionType = params.getOrElse("action.type", "train")
    val json = params.getOrElse("angel.ml.conf", "jsons/logreg.json")

    SharedConf.get().set("angel.ml.conf", json)

    SharedConf.addMap(params)
    JsonUtils.init()

    // load data
    val conf = new SparkConf()
    conf.setMaster("local[1]")
    conf.setAppName("Json Test")
    conf.set("spark.ps.model", "LOCAL")
    conf.set("spark.ps.jars", "")
    conf.set("spark.ps.instances", "1")
    conf.set("spark.ps.cores", "1")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    PSContext.getOrCreate(sc)

    val model = new GraphModel
    val dim = SharedConf.indexRange.toInt
    val learner = new OfflineLearner

    println(s"dim=$dim")

    actionType match {
      case MLConf.ANGEL_ML_TRAIN => learner.train(input, output, modelPath, dim, model)
      case MLConf.ANGEL_ML_PREDICT => learner.train(input, output, modelPath, dim, model)
    }

    PSContext.stop()
    sc.stop()
  }

}