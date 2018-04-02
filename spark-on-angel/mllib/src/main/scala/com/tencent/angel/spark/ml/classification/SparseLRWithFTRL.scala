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


import it.unimi.dsi.fastutil.longs.{Long2DoubleMap, Long2DoubleOpenHashMap}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import com.tencent.angel.spark.context.{AngelPSContext, PSContext}
import com.tencent.angel.spark.linalg.{BLAS, OneHotVector, SparseVector}
import com.tencent.angel.spark.ml.optimize.{FTRL, OWLQN}
import com.tencent.angel.spark.ml.util.{ArgsUtil, DataLoader}
import com.tencent.angel.spark.models.vector.{PSVector, SparsePSVector}
import com.tencent.angel.utils.MurmurHash3

object SparseLRWithFTRL {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params("input")
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val epoch = params.getOrElse("epoch", "5").toInt
    val pDim = params.getOrElse("dim", "-1").toDouble.toInt
    val lambda1 = params.getOrElse("lambda1", "0.1").toDouble
    val lambda2 = params.getOrElse("lambda2", "0.1").toDouble
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

    val tempInstances = DataLoader.loadOneHotInstance(input, partitionNum, sampleRate, -1).rdd
      .map { row =>
        Tuple2(row.getAs[scala.collection.mutable.WrappedArray[Long]](1).toArray, row.getString(0).toDouble)
      }.map { case (feat, label) =>
        val hashed = feat.map { index =>
          val bytes = f"$index%4d".getBytes
          MurmurHash3.murmurhash3_x64_64(bytes, bytes.length, 41)
        }
        (0L +: hashed, label)
      }

    val dim = if (pDim > 0) {
      pDim
    } else {
      tempInstances.flatMap { case (feat, label) => feat.distinct}.distinct().count()
    }

    println(s"feat number: $dim")

    val instances = tempInstances.map { case (feat, label) => (new OneHotVector(dim, feat), label) }
    instances.cache()
    val sampleNum = instances.count()
    val posSamples = instances.filter(_._2 == 1.0).count()
    val negSamples = instances.filter(_._2 == 0.0).count()
    println(s"total count: $sampleNum posSample: $posSamples negSamples: $negSamples")
    require(posSamples + negSamples == sampleNum, "labels must be 0 or 1")

    train(instances, sampleNum, batchSize, dim, lambda1, lambda2, alpha, beta, epoch)
  }

  def train(
      trainData: RDD[(OneHotVector, Double)],
      sampleNum: Long,
      batchSize: Int,
      dim: Long,
      lambda1: Double,
      lambda2: Double,
      alpha: Double,
      beta: Double,
      epoch: Int): Unit = {

    def getGradLoss(w: SparseVector, label: Double, feature: OneHotVector): (SparseVector, Double) = {
      val margin = -1 * BLAS.dot(w, feature)
      val gradientMultiplier = 1.0 / (1.0 + math.exp(margin)) - label
      val grad = new SparseVector(w.length)
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

    def plusTo(a: SparseVector, b: SparseVector): Unit = {
      val iter = a.keyValues.long2DoubleEntrySet().fastIterator()
      var entry: Long2DoubleMap.Entry = null
      while(iter.hasNext) {
        entry = iter.next()
        b.keyValues.addTo(entry.getLongKey, entry.getDoubleValue)
      }
    }

    val initWeightPS = PSVector.longKeySparse(dim, -1, 5)
    val zPS = PSVector.duplicate(initWeightPS)
    val nPS = PSVector.duplicate(initWeightPS)

    val ftrl = new FTRL(lambda1, lambda2, alpha, beta)

    val partNum = trainData.partitions.length
    val instNumPerPart = batchSize / partNum
    val batchNum = trainData.count().toInt / batchSize
    println(s"epoch num: $epoch batch size: $batchSize partition num: $partNum")
    println(s"For each partition, batch num: $batchNum batch size: $instNumPerPart")

    (0 until epoch).foreach { epochId =>
      trainData.mapPartitions { iter =>
        var batchId = 0
        iter.toArray.sliding(instNumPerPart, instNumPerPart)
          .map { batch =>
            val featIds = batch.flatMap { case (feat, label) => feat.indices }.distinct

            ftrl.updateState(zPS, nPS, featIds)
            val deltaZ = new SparseVector(dim, featIds.length)
            val deltaN = new SparseVector(dim, featIds.length)

            val lossSum = batch.map { case (feature, label) =>
              val (littleZ, littleN, loss) = ftrl.optimize(feature, label, getGradLoss)
              plusTo(littleN, deltaN)
              plusTo(littleZ, deltaZ)
              loss
            }.sum
            zPS.increment(deltaZ)
            nPS.increment(deltaN)

            if (batchId % 10 == 0) {
              println(s"batch id: $batchId")
              //println(s"epoch $epochId batchId: $batchId zPS nnz: ${zPS.toBreeze.norm(0)} nPS nnz: ${nPS.toBreeze.norm(0)}")
              // val sampleNum = instNumPerPart * partNum
              // println(s"epoch $epochId batchId: $batchId loss ${lossSum / sampleNum}")
            }
            batchId += 1
            Thread.sleep(30 * 1000)
            lossSum
          }
      }.count()

      println(s"zPS nnz: ${zPS.toBreeze.norm(0)} nPS nnz: ${nPS.toBreeze.norm(0)}")
      println(s"finish epoch $epochId")

//      ftrl.updateState(zPS, nPS)
//      val localWeight = ftrl.weight
//      println(s"weights nnz: ${localWeight.nnz} index: ${localWeight.indices.take(10).mkString(" ")} " +
//        s"value: ${localWeight.values.take(10).mkString(" ")}")
//      println(s"inception: ${localWeight(0)}")
    }
  }
}

