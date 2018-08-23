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

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.spark.client.PSClient
import org.apache.commons.logging.LogFactory
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

class OnlineLearner {

  // Shared configuration with Angel-PS
  val conf = SharedConf.get()
  val LOG = LogFactory.getLog(classOf[OnlineLearner])

  def train(stream: DStream[LabeledData], model: GraphModel, numTasks: Int): Unit = {
    model.init(numTasks)
    val bModel = SparkContext.getOrCreate().broadcast(model)
    var numBatches = 0
    stream.foreachRDD { data =>
      numBatches += 1
      val (lossSum, batchSize) = data.mapPartitions { case iter =>
        PSClient.instance()
        val model = bModel.value
        val samples = iter.toArray
        model.forward(samples)
        val loss = model.getLoss()
        model.backward()
        Iterator.single((loss, samples.size))
      }.reduce((f1, f2) => (f1._1 + f2._1, f1._2 + f2._2))

      model.update(numBatches)

      LOG.error(s"batch[$numBatches] trainLoss=${lossSum / numTasks} batchSize=$batchSize")
    }
  }

}
