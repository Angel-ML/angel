/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.spark.ml.classification

import org.apache.spark.SparkConf
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import com.tencent.angel.spark.context.{AngelPSContext, PSContext}
import com.tencent.angel.spark.linalg.{BLAS, OneHotVector, SparseVector}
import com.tencent.angel.spark.ml.optimize.FTRL
import com.tencent.angel.spark.ml.util.{ArgsUtil, DataLoader}
import com.tencent.angel.spark.models.vector.cache.PullMan
import com.tencent.angel.utils.MurmurHash3

object SparseLRWithFTRL {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params("input")
    val modelPath = params("modelPath")
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val validateFaction = params.getOrElse("validateFaction", "0.3").toDouble
    val epoch = params.getOrElse("epoch", "5").toInt
    val pDim = params.getOrElse("dim", "-1").toDouble.toInt
    val lambda1 = params.getOrElse("lambda1", "1.0").toDouble
    val lambda2 = params.getOrElse("lambda2", "1.0").toDouble
    val alpha = params.getOrElse("alpha", "0.1").toDouble
    val beta = params.getOrElse("beta", "1.0").toDouble
    val batchSize = params.getOrElse("batchSize", "10000").toInt

    val conf = new SparkConf()
      .setMaster(mode)
      .setAppName(this.getClass.getSimpleName)

    AngelPSContext.adjustExecutorJVM(conf)
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    PSContext.getOrCreate(spark.sparkContext)

    val tempInstances = DataLoader.loadOneHotInstance(input, partitionNum, sampleRate).rdd
      .map { row =>
        Tuple2(row.getAs[scala.collection.mutable.WrappedArray[Long]](1).toArray, row.getString(0).toDouble)
      }.map { case (feat, label) =>
        (0L +: feat, label)
      }

    val dim = if (pDim > 0) {
      pDim
    } else {
      tempInstances.flatMap { case (feat, label) => feat.distinct }.distinct().count()
    }

    println(s"feat number: $dim")

    val instances = tempInstances.map { case (feat, label) => (new OneHotVector(dim, feat), label) }
    instances.cache()
    val sampleNum = instances.count()
    val posSamples = instances.filter(_._2 == 1.0).count()
    val negSamples = instances.filter(_._2 == 0.0).count()
    println(s"total count: $sampleNum posSample: $posSamples negSamples: $negSamples")
    require(posSamples + negSamples == sampleNum, "labels must be 0 or 1")

    val model = train(instances, sampleNum, dim, epoch, batchSize, lambda1, lambda2, alpha, beta, validateFaction)
    model.save(modelPath)
  }

  def train(
      instances: RDD[(OneHotVector, Double)],
      sampleNum: Long,
      dim: Long,
      epoch: Int,
      batchSize: Int,
      lambda1: Double,
      lambda2: Double,
      alpha: Double,
      beta: Double,
      validateFaction: Double): SparseLRModel = {

    val (trainSet, validateSet) = if (validateFaction > 0 && validateFaction < 1.0) {
      val rdds = instances.randomSplit(Array(1 - validateFaction, validateFaction))
      (rdds(0), rdds(1))
    } else {
      (instances, null)
    }

    println(s"epoch num: $epoch batch size: $batchSize")
    val ftrl = new FTRL(lambda1, lambda2, alpha, beta)
    ftrl.initPSModel(dim)

    (0 until epoch).foreach { epochId =>
      val tempRDD = trainSet.mapPartitions { iter =>
        var batchId = 0
        val instances = iter.toArray
        instances.sliding(batchSize, batchSize)
          .map { batch =>
            val batchLoss = ftrl.optimize(batch, calcGradientLoss)
            if (batchId % 10 == 0) {
               println(s"epoch $epochId batchId: $batchId loss $batchLoss")
            }
            batchId += 1
            batchLoss
          }
        }
      tempRDD.count()

      val wPS = ftrl.weight
      val lrModel = SparseLRModel(wPS)

      println(s"epoch: $epochId model info: ${lrModel.simpleInfo}")
      println(s"epoch: $epochId zPS nnz: ${ftrl.zPS.toBreeze.norm(0)} nPS nnz: ${ftrl.nPS.toBreeze.norm(0)}")
      val (loss, auc) = evaluate(trainSet, lrModel)
      println(s"epoch: $epochId train global loss: $loss auc: $auc")
      if (validateSet != null) {
        val (validateLoss, validateAuc) = evaluate(validateSet, lrModel)
        println(s"epoch: $epochId validate global loss: $validateLoss auc: $validateAuc")
      }
    }

    SparseLRModel(ftrl.weight)
  }


  def evaluate(evalRDD: RDD[(OneHotVector, Double)], lrModel: SparseLRModel): (Double, Double) = {

    val tempRDD = evalRDD.mapPartitions { iter =>
      val localW = lrModel.w.toCache.pullFromCache().toSparse
      iter.map { case (feature, label) =>
        val loss = calcLoss(localW, label, feature)
        val prob = lrModel.predict(feature, localW)
        Tuple3(loss, prob, label)
      }
    }.cache()
    PullMan.release(lrModel.w)

    val loss = tempRDD.map(_._1).mean()

    val binEvaluator = new BinaryClassificationMetrics(tempRDD.map(x => Tuple2(x._2, x._3)))
    val auc = binEvaluator.areaUnderROC()
    tempRDD.unpersist(false)
    (loss, auc)
  }

  private def calcLoss(w: SparseVector, label: Double, feature: OneHotVector): Double = {
    val margin = -1 * BLAS.dot(w, feature)
    val loss = if (label > 0) {
      math.log1p(math.exp(margin))
    } else {
      math.log1p(math.exp(margin)) - margin
    }
    loss
  }

  private def calcGradientLoss(w: SparseVector, label: Double, feature: OneHotVector): (SparseVector, Double) = {
    val margin = -1 * BLAS.dot(w, feature)
    val gradientMultiplier = 1.0 / (1.0 + math.exp(margin)) - label
    val grad = new SparseVector(w.length, feature.indices.length)
    feature.indices.foreach { fId =>
      grad.put(fId, gradientMultiplier)
    }

    val loss = if (label > 0) {
      math.log1p(math.exp(margin))
    } else {
      math.log1p(math.exp(margin)) - margin
    }
    (grad, loss)
  }

}

