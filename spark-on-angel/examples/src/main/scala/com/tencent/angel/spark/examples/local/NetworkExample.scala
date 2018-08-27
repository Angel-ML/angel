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
import com.tencent.angel.ml.core.utils.DataParser
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.{ArgsUtil, GraphModel, OfflineLearner}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}

object NetworkExample {
  PropertyConfigurator.configure("conf/log4j.properties")

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val input = params.getOrElse("input", "data/census/census_148d_train.dummy")
    val dataType = params.getOrElse(MLConf.ML_DATA_INPUT_FORMAT, "dummy")
    val features = params.getOrElse(MLConf.ML_FEATURE_INDEX_RANGE, "148").toInt
    val numField = params.getOrElse(MLConf.ML_FIELD_NUM, "13").toInt
    val numRank = params.getOrElse(MLConf.ML_RANK_NUM, "8").toInt
    val numEpoch = params.getOrElse(MLConf.ML_EPOCH_NUM, "200").toInt
    val fraction = params.getOrElse(MLConf.ML_BATCH_SAMPLE_RATIO, "0.1").toDouble
    val lr = params.getOrElse(MLConf.ML_LEARN_RATE, "0.02").toDouble

    val network = params.getOrElse("network", "LogisticRegression")

    SharedConf.addMap(params)
    SharedConf.get().set(MLConf.ML_DATA_INPUT_FORMAT, dataType)
    SharedConf.get().setInt(MLConf.ML_FEATURE_INDEX_RANGE, features)
    SharedConf.get().setInt(MLConf.ML_FIELD_NUM, numField)
    SharedConf.get().setInt(MLConf.ML_RANK_NUM, numRank)
    SharedConf.get().setInt(MLConf.ML_EPOCH_NUM, numEpoch)
    SharedConf.get().setDouble(MLConf.ML_BATCH_SAMPLE_RATIO, fraction)
    SharedConf.get().setDouble(MLConf.ML_LEARN_RATE, lr)


    val className = "com.tencent.angel.spark.ml.classification." + network
    val model = GraphModel(className)
    val learner = new OfflineLearner()

    // load data
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName(s"$network Example")
    conf.set("spark.ps.model", "LOCAL")
    conf.set("spark.ps.instances", "1")
    conf.set("spark.ps.cores", "1")
    conf.set("spark.ui.enabled", "false")

    val sc = new SparkContext(conf)
    val parser = DataParser(SharedConf.get())
    val data = sc.textFile(input).map(f => parser.parse(f))

    PSContext.getOrCreate(sc)

    learner.train(data, model)

    PSContext.stop()
    sc.stop()
  }

}
