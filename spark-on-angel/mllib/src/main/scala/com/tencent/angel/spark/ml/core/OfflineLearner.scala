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

import scala.util.Random


class OfflineLearner {

  // Shared configuration with Angel-PS
  val conf = SharedConf.get()

  // Some params
  var numEpoch: Int = conf.getInt(MLConf.ML_EPOCH_NUM)
  var fraction: Double = conf.getDouble(MLConf.ML_BATCH_SAMPLE_RATIO)
  var validationRatio: Double = conf.getDouble(MLConf.ML_VALIDATE_RATIO)
  var batchSize: Int = conf.getInt(MLConf.ML_MINIBATCH_SIZE)

  println(s"fraction=$fraction validateRatio=$validationRatio numEpoch=$numEpoch")

  def evaluate(data: RDD[LabeledData], bModel: Broadcast[GraphModel]): (Double, Double) = {
    val scores = data.mapPartitions { case iter =>
      val model = bModel.value
      val output = model.forward(1, iter.toArray)
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

    val bModel = SparkContext.getOrCreate().broadcast(model)

    val manifold = OfflineLearner.buildManifold(train, fraction)
    var numUpdate = 1
    for (epoch <- 0 until numEpoch) {

      val batchIterator = OfflineLearner.buildManifoldIterator(manifold, fraction)
      while (batchIterator.hasNext) {
        val (sumLoss, batchSize) = batchIterator.next().mapPartitions { case iter =>
          PSClient.instance()
          val batch = iter.next()
          bModel.value.forward(epoch, batch)
          val loss = bModel.value.getLoss()
          bModel.value.backward()
          Iterator.single((loss, batch.length))
        }.reduce((f1, f2) => (f1._1 + f2._1, f1._2 + f2._2))

        val (lr, boundary) = model.update(numUpdate, batchSize)
        val loss = sumLoss / model.graph.taskNum
        println(s"epoch=[$epoch] lr[$lr] batchSize[$batchSize] trainLoss=$loss")
        if (boundary) {
          var validateMetricLog = ""
          if (validationRatio > 0.0) {
            val (validateAuc, validatePrecision) = evaluate(validate, bModel)
            validateMetricLog = s"validateAuc=$validateAuc validatePrecision=$validatePrecision"
          }
          println(s"batch[$numUpdate] $validateMetricLog")
        }

        numUpdate += 1
      }
    }
  }

  def predict(data: RDD[LabeledData], model: GraphModel): Unit = {

  }

}

object OfflineLearner {

  def buildManifold(data: RDD[LabeledData], fraction: Double): RDD[Array[LabeledData]] = {
    val batchNum = (1.0 / fraction).toInt

    def shuffleAndSplit(iterator: Iterator[LabeledData]): Iterator[Array[LabeledData]] = {
      val samples = Random.shuffle(iterator).toArray
      val sizes = Array.tabulate(batchNum)(_ => samples.length / batchNum)
      val left = samples.length % batchNum
      for (i <- 0 until left) sizes(i) += 1

      var idx = 0
      val manifold = new Array[Array[LabeledData]](batchNum)
      for (a <- 0 until batchNum) {
        manifold(a) = new Array[LabeledData](sizes(a))
        for (b <- 0 until sizes(a)) {
          manifold(a)(b) = samples(idx)
          idx += 1
        }
      }
      manifold.iterator
    }

    val manifold = data.mapPartitions(it => shuffleAndSplit(it))
    manifold.cache()
    manifold.count()
    manifold
  }

  def skip(partitionId: Int, iterator: Iterator[Array[LabeledData]], skipNum: Int): Iterator[Array[LabeledData]] = {
    (0 until skipNum).foreach(_ => iterator.next())
    Iterator.single(iterator.next())
  }

  def buildManifoldIterator(manifold: RDD[Array[LabeledData]], fraction: Double): Iterator[RDD[Array[LabeledData]]] = {
    val batchNum = (1.0 / fraction).toInt

    new Iterator[RDD[Array[LabeledData]]] with Serializable {
      var index = 0

      override def hasNext(): Boolean = index < batchNum

      override def next(): RDD[Array[LabeledData]] = {
        val batch = manifold.mapPartitionsWithIndex((partitionId, it) => skip(partitionId, it, index), true)
        index += 1
        batch
      }
    }

  }
}
