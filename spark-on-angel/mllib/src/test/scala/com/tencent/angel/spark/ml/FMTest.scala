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


package com.tencent.angel.spark.ml

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.utils.DataParser
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.metric.Precision
import com.tencent.angel.spark.ml.core.{ArgsUtil, GraphModel, OfflineLearner}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}

object FMTest {

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("angel-ps/conf/log4j.properties")
    val params = ArgsUtil.parse(args)

    val input = params.getOrElse("input", "./spark-on-angel/mllib/src/test/data/census.train")
    val actionType = params.getOrElse("actionType", "train")

    // build SharedConf with params
    SharedConf.get()
    SharedConf.addMap(params)
    SharedConf.get().set(MLConf.ML_MODEL_TYPE, RowType.T_FLOAT_DENSE.toString)
    SharedConf.get().setInt(MLConf.ML_FEATURE_INDEX_RANGE, 148)
    SharedConf.get().set(MLConf.ML_DATA_INPUT_FORMAT, "dummy")
    SharedConf.get().setInt(MLConf.ML_EPOCH_NUM, 200)
    SharedConf.get().setDouble(MLConf.ML_VALIDATE_RATIO, 0.0)
    SharedConf.get().setDouble(MLConf.ML_REG_L2, 0.001)
    SharedConf.get().setDouble(MLConf.ML_BATCH_SAMPLE_RATIO, 0.2)
    SharedConf.get().setInt(MLConf.ML_RANK_NUM, 5)
    SharedConf.get().setLong(MLConf.ML_FIELD_NUM, 13)


    val className = "com.tencent.angel.spark.ml.classification.FactorizationMachine"
    val model = GraphModel(className)
    val learner = new OfflineLearner()

    // load data
    val conf = new SparkConf()
    conf.setMaster("local[1]")
    conf.setAppName("FM Test")
    conf.set("spark.ps.model", "LOCAL")
    conf.set("spark.ps.jars", "")
    conf.set("spark.ps.instances", "1")
    conf.set("spark.ps.cores", "1")

    val sc = new SparkContext(conf)
    val parser = DataParser(SharedConf.get())
    val data = sc.textFile(input).repartition(1).map(f => parser.parse(f))


    PSContext.getOrCreate(sc)

    actionType match {
      case "train" =>
        learner.train(data, model)
        model.save("")
      case "predict" =>
        model.load("")
        learner.predict(data, model)
      case _ =>
        throw new AngelException("actionType should be train or predict")
    }

    PSContext.stop()
    sc.stop()
  }

}
