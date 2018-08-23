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


package com.tencent.angel.spark.ml.core

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.ml.core.metric.{AUC, Precision}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD


class OfflineLearner {

  // Shared configuration with Angel-PS
  val conf = SharedConf.get()

  // Some params
  var maxIteration: Int = conf.getInt(MLConf.ML_EPOCH_NUM)
  var fraction: Double = conf.getDouble(MLConf.ML_BATCH_SAMPLE_RATIO)
  var validationRatio: Double = conf.getDouble(MLConf.ML_VALIDATE_RATIO)

  println(s"fraction=$fraction validateRatio=$validationRatio maxIteration=$maxIteration")

  def evaluate(data: RDD[LabeledData], bModel: Broadcast[GraphModel]): (Double, Double) = {
    val scores = data.mapPartitions { case iter =>
      val model = bModel.value
      val output = model.forward(iter.toArray)
      Iterator.single((output, model.graph.placeHolder.getLabel))
    }
    (new AUC().cal(scores), new Precision().cal(scores))
  }

  def train(data: RDD[LabeledData], model: GraphModel): Unit = {
    // split data into train and validate
    val ratios = Array(1 - validationRatio, validationRatio)
    val splits = data.randomSplit(ratios)
    val train = splits(0)
    val validate = splits(1)

    data.cache()

    train.cache()
    validate.cache()

    train.count()
    validate.count()

    data.unpersist()

    // build network
    model.init(train.getNumPartitions)

    val bModel = SparkContext.getOrCreate().broadcast(model)

    var (start, end) = (0L, 0L)
    var (reduceTime, updateTime) = (0L, 0L)

    for (iteration <- 1 to maxIteration) {
      start = System.currentTimeMillis()
      val (lossSum, batchSize) = train.sample(false, fraction, 42 + iteration)
        .mapPartitions { case iter =>
          PSClient.instance()
          val samples = iter.toArray
          bModel.value.forward(samples)
          val loss = bModel.value.getLoss()
          bModel.value.backward()
          Iterator.single((loss, samples.size))
        }.reduce((f1, f2) => (f1._1 + f2._1, f1._2 + f2._2))
      end = System.currentTimeMillis()
      reduceTime = end - start

      start = System.currentTimeMillis()
      model.update(iteration)
      end = System.currentTimeMillis()
      updateTime = end - start

      val loss = lossSum / model.graph.taskNum

      println(s"batch[$iteration] batchSize=$batchSize trainLoss=$loss reduceTime=$reduceTime updateTime=$updateTime")

      val numEvaluate = SparkContext.getOrCreate().getConf.getInt("spark.offline.evaluate", 50)

      if (iteration % numEvaluate == 0) {
        val (trainAuc, trainPrecision) = evaluate(train, bModel)
        val trainMetricLog = s"trainAuc=$trainAuc trainPrecision=$trainPrecision"
        //        val trainMetricLog = ""
        var validateMetricLog = ""
        if (validationRatio > 0.0) {
          val (validateAuc, validatePrecision) = evaluate(validate, bModel)
          validateMetricLog = s"validateAuc=$validateAuc validatePrecision=$validatePrecision"
        }
        println(s"batch[$iteration] $trainMetricLog $validateMetricLog")
      }
    }
  }

  def predict(data: RDD[LabeledData], model: GraphModel): Unit = {
    // build network
    model.init(data.getNumPartitions)
    val bModel = SparkContext.getOrCreate().broadcast(model)
    data.mapPartitions { case iter =>
      val model = bModel.value
      val output = model.forward(iter.toArray)
      Iterator.single(output)
    }
  }

}
