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

import com.tencent.angel.RunningMode
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.utils.DataParser
import com.tencent.angel.ml.core.utils.paramsutils.JsonUtils
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.{ArgsUtil, GraphModel, OfflineLearner}
import com.tencent.angel.spark.ml.util.{Features, ModelLoader, ModelSaver}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}
import org.codehaus.jackson.JsonParser.Feature

object JsonExample {

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("conf/log4j.properties")
    val params = ArgsUtil.parse(args)
    val input = params.getOrElse("input", "data/census/census_148d_train.libsvm")
    val output = params.getOrElse("output", "output/")
    val modelPath = params.getOrElse("model", "model/")
    val actionType = params.getOrElse("action.type", "train")

    SharedConf.addMap(params)
    SharedConf.get().set(AngelConf.ANGEL_RUNNING_MODE, RunningMode.ANGEL_PS.toString)

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
    val parser = DataParser(SharedConf.get())
    val data = sc.textFile(input).repartition(1).map(f => parser.parse(f))
    PSContext.getOrCreate(sc)

    val model = new GraphModel
    val learner = new OfflineLearner

    // initialize model
    model.init(data.getNumPartitions)
    // load model if there exists
    model.load(modelPath)
    // model training
    learner.train(data, model)
    // save it
    model.save(output)

    PSContext.stop()
    sc.stop()
  }

}
