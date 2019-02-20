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
import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.utils.DataParser
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.metric.Precision
import com.tencent.angel.spark.ml.core.{ArgsUtil, GraphModel, OfflineLearner}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}

object FMTest {

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("angel-ps/conf/log4j.properties")
    val params = ArgsUtil.parse(args)

    val input = params.getOrElse("input", "./data/census/census_148d_train.libsvm")
    val modelInput = params.getOrElse("model", "")
    val modelOutput = params.getOrElse("output", "")
    val actionType = params.getOrElse("actionType", "train")

    // build SharedConf with params
    SharedConf.get()
    SharedConf.addMap(params)
    SharedConf.get().set(MLCoreConf.ML_MODEL_TYPE, RowType.T_FLOAT_DENSE.toString)
    SharedConf.get().setInt(MLCoreConf.ML_FEATURE_INDEX_RANGE, 148)
    SharedConf.get().set(MLCoreConf.ML_DATA_INPUT_FORMAT, "libsvm")
    SharedConf.get().setInt(MLCoreConf.ML_EPOCH_NUM, 200)
    SharedConf.get().setDouble(MLCoreConf.ML_VALIDATE_RATIO, 0.0)
    SharedConf.get().setDouble(MLCoreConf.ML_REG_L2, 0.001)
    SharedConf.get().setDouble(MLCoreConf.ML_BATCH_SAMPLE_RATIO, 0.2)
    SharedConf.get().setInt(MLCoreConf.ML_RANK_NUM, 5)
    SharedConf.get().setLong(MLCoreConf.ML_FIELD_NUM, 13)


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
    PSContext.getOrCreate(sc)
    val dim = SharedConf.indexRange.toInt

    actionType match {
      case "train" =>
        learner.train(input, modelOutput, modelInput, dim, model)

      case "com/tencent/angel/ml/predict" =>
        learner.predict(input, modelOutput, modelInput, dim, model)
      case _ =>
        throw new AngelException("actionType should be train or predict")
    }
    PSContext.stop()
    sc.stop()
  }

}
