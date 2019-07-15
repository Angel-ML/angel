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
import com.tencent.angel.ml.core.optimizer.loss.{L2Loss, LogLoss}
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.matrix.{BlasDoubleMatrix, BlasFloatMatrix}
import com.tencent.angel.spark.context.AngelPSContext.convertToHadoop
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.metric.{AUC, Precision}
import com.tencent.angel.spark.ml.util.{DataLoader, SparkUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.Random

class OfflineLearner {

  // Shared configuration with Angel-PS
  val sharedConf: SharedConf = SharedConf.get()

  // Some params
  var numEpoch: Int = sharedConf.getInt(MLConf.ML_EPOCH_NUM)
  var fraction: Double = sharedConf.getDouble(MLConf.ML_BATCH_SAMPLE_RATIO)
  var validationRatio: Double = sharedConf.getDouble(MLConf.ML_VALIDATE_RATIO)

  println(s"fraction=$fraction validateRatio=$validationRatio numEpoch=$numEpoch")

  def evaluate(data: RDD[LabeledData], model: GraphModel): (Double, Double) = {
    val scores = data.mapPartitions { case iter =>
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

    val numSplits = (1.0 / fraction).toInt
    val manifold = OfflineLearner.buildManifold(train, numSplits)

    train.unpersist()
    
    var numUpdate = 1

    for (epoch <- 0 until numEpoch) {
      val batchIterator = OfflineLearner.buildManifoldIterator(manifold, numSplits)
      while (batchIterator.hasNext) {
        val (sumLoss, batchSize) = batchIterator.next().mapPartitions { case iter =>
          PSContext.instance()
          val batch = iter.next()
          model.forward(epoch, batch)
          val loss = model.getLoss()
          model.backward()
          Iterator.single((loss, batch.length))
        }.reduce((f1, f2) => (f1._1 + f2._1, f1._2 + f2._2))

        val (lr, boundary) = model.update(numUpdate, batchSize)
        val loss = sumLoss / model.graph.taskNum
        println(s"epoch=[$epoch] lr[$lr] batchSize[$batchSize] trainLoss=$loss")
        if (boundary) {
          var validateMetricLog = ""
          if (validationRatio > 0.0) {
            val (validateAuc, validatePrecision) = evaluate(validate, model)
            validateMetricLog = s"validateAuc=$validateAuc validatePrecision=$validatePrecision"
          }
          println(s"batch[$numUpdate] $validateMetricLog")
        }
        numUpdate += 1
      }
    }
  }

  /**
    * Predict the output with a RDD with data and a trained model
    * @param data: examples to be predicted
    * @param model: a trained model
    * @return RDD[(label, predict value)]
    *
    */
  def predict(data: RDD[(LabeledData, String)], model: GraphModel): RDD[(String, Double)] = {
    val scores = data.mapPartitions { iterator =>
      PSContext.instance()
      val samples = iterator.toArray
      val output  = model.forward(1, samples.map(f => f._1))
      val labels = samples.map(f => f._2)

      (output, model.getLossFunc()) match {
        case (mat :BlasDoubleMatrix, _: LogLoss) =>
          // For LogLoss, the output is (value, sigmoid(value), label)
          (0 until mat.getNumRows).map(idx => (labels(idx), mat.get(idx, 1))).iterator
        case (mat :BlasFloatMatrix, _: LogLoss) =>
          // For LogLoss, the output is (value, sigmoid(value), label)
          (0 until mat.getNumRows).map(idx => (labels(idx), mat.get(idx, 1).toDouble)).iterator
        case (mat: BlasDoubleMatrix , _: L2Loss) =>
          // For L2Loss, the output is (value, _, _)
          (0 until mat.getNumRows).map(idx => (labels(idx), mat.get(idx, 0))).iterator
        case (mat: BlasFloatMatrix , _: L2Loss) =>
          // For L2Loss, the output is (value, _, _)
          (0 until mat.getNumRows).map(idx => (labels(idx), mat.get(idx, 0).toDouble)).iterator
      }
    }
    scores
  }

  def train(input: String,
            modelOutput: String,
            modelInput: String,
            dim: Int,
            model: GraphModel): Unit = {
    val conf = SparkContext.getOrCreate().getConf
    val data = SparkContext.getOrCreate().textFile(input)
      .repartition(SparkUtils.getNumCores(conf))
      .map(f => DataLoader.parseIntFloat(f, dim))

    model.init(data.getNumPartitions)

    if (modelInput.length > 0) model.load(modelInput)
    train(data, model)
    if (modelOutput.length > 0) {
      model.save(modelOutput)
      model.saveJson(modelOutput, convertToHadoop(conf))
    }
  }

  def predict(input: String,
              output: String,
              modelInput: String,
              dim: Int,
              model: GraphModel): Unit = {
    val dataWithLabels = SparkContext.getOrCreate().textFile(input)
      .map(f => (DataLoader.parseIntFloat(f, dim), DataLoader.parseLabel(f)))

    model.init(dataWithLabels.getNumPartitions)
    model.load(modelInput)

    val predicts = predict(dataWithLabels, model)
    if (output.length > 0)
      predicts.map(f => s"${f._1} ${f._2}").saveAsTextFile(output)
  }

}

object OfflineLearner extends Serializable {

  /**
    * Build manifold view for a RDD. A manifold RDD is to split a RDD to multiple RDD.
    * First, we shuffle the RDD and split it into several splits inside every partition.
    * Then, we hold the manifold RDD into cache.
    * @param data, RDD to be split
    * @param numSplit, the number of splits
    * @return
    */
  def buildManifold[T: ClassTag](data: RDD[T], numSplit: Int): RDD[Array[T]] = {
    def shuffleAndSplit(iterator: Iterator[T]): Iterator[Array[T]] = {
      val samples = Random.shuffle(iterator).toArray
      val sizes = Array.tabulate(numSplit)(_ => samples.length / numSplit)
      val left = samples.length % numSplit
      for (i <- 0 until left) sizes(i) += 1

      var idx = 0
      val manifold = new Array[Array[T]](numSplit)
      for (a <- 0 until numSplit) {
        manifold(a) = new Array[T](sizes(a))
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

  /**
    * Return an iterator for the manifold RDD. Each element returned by the iterator is a RDD
    * which contains a split for the manifold RDD.
    * @param manifold, RDD to be split
    * @param numSplit, number of splits to split the manifold RDD
    * @return
    */
  def buildManifoldIterator[T: ClassTag](manifold: RDD[Array[T]], numSplit: Double): Iterator[RDD[Array[T]]] = {

    def skip[T](partitionId: Int, iterator: Iterator[Array[T]], skipNum: Int): Iterator[Array[T]] = {
      (0 until skipNum).foreach(_ => iterator.next())
      Iterator.single(iterator.next())
    }

    new Iterator[RDD[Array[T]]] with Serializable {
      var index = 0

      override def hasNext(): Boolean = index < numSplit

      override def next(): RDD[Array[T]] = {
        val batch = manifold.mapPartitionsWithIndex((partitionId, it) => skip(partitionId, it, index - 1), true)
        index += 1
        batch
      }
    }

  }
}
