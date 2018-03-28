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

import scala.collection.mutable.ArrayBuffer
import scala.collection.Map
import scala.util.Random

import breeze.optimize.{CachedDiffFunction, DiffFunction}
import com.yahoo.sketches.quantiles.DoublesSketch
import it.unimi.dsi.fastutil.longs.{Long2DoubleMap, Long2DoubleOpenHashMap}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import com.tencent.angel.spark.context.{AngelPSContext, PSContext}
import com.tencent.angel.spark.linalg.{BLAS, OneHotVector, SparseVector}
import com.tencent.angel.spark.ml.optimize.OWLQN
import com.tencent.angel.spark.ml.psf.ADMMZUpdater
import com.tencent.angel.spark.ml.util.{ArgsUtil, DataLoader}
import com.tencent.angel.spark.models.vector.{PSVector, SparsePSVector}
import com.tencent.angel.spark.models.vector.cache.PullMan
import com.tencent.angel.spark.models.vector.enhanced.BreezePSVector
import com.tencent.angel.utils.MurmurHash3

object SparseLRWithOWLQN {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params("input")
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val m = params.getOrElse("m", "10").toInt
    val maxIter = params.getOrElse("maxIter", "50").toInt
    val regParam = params.getOrElse("regParam", "0.0001").toDouble
    val elasticNet = params.getOrElse("elasticNet", "1.0").toDouble
    val pDim = params.getOrElse("dim", "-1").toDouble.toInt
    val truncThreshold = params.getOrElse("truncThreshold", "1e-7").toDouble
    val sampleHitRate = params.getOrElse("sampleHitRate", "0.7").toDouble

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

    // calculate distinct feature count for each partition
    val part2FeatCountMap = instances.mapPartitionsWithIndex { case (pId, iter) =>
      val indices = iter.flatMap { case (feat, label) => feat.indices }
      val partFeatNum = indices.toArray.distinct.length
      Iterator.single(Tuple2(pId, partFeatNum))
    }.collectAsMap()
    val bcPart2FeatCountMap = spark.sparkContext.broadcast(part2FeatCountMap)

    val l1Reg = elasticNet * regParam
    val l2Reg = (1 - elasticNet) * regParam

    runSparsePSOWLQN(instances, bcPart2FeatCountMap, sampleNum, dim,
      sampleHitRate, m, l1Reg, l2Reg, truncThreshold, maxIter)
  }

  def runSparsePSOWLQN(
      trainData: RDD[(OneHotVector, Double)],
      bcPart2FeatCountMap: Broadcast[Map[Int, Int]],
      sampleNum: Long,
      dim: Long,
      sampleHitRate: Double,
      m: Int,
      l1Reg: Double,
      l2Reg: Double,
      truncThreshold: Double,
      maxIter: Int): Unit = {

    val initWeightPS = PSVector.longKeySparse(dim, -1, 2 * m + 20).toBreeze

    val owlqn = new OWLQN(maxIter, m, l1Reg)
    val ff = new CachedDiffFunction[BreezePSVector](SparsePSCost(trainData, bcPart2FeatCountMap,
      sampleNum, sampleHitRate, l1Reg, l2Reg, truncThreshold))
    val states = owlqn.iterations(ff, initWeightPS)

    val lossHistory = new ArrayBuffer[Double]()
    var weight: BreezePSVector = null
    while (states.hasNext) {
      val state = states.next()
      lossHistory += state.value

      if (!states.hasNext) {
        weight = state.x
      }
    }

    println(s"loss history: ${lossHistory.toArray.mkString(" ")}")
    val localWeight = weight.pull.toSparse
    println(s"weights nnz: ${localWeight.nnz} " +
      s"index: ${localWeight.indices.take(10).mkString(" ")} " +
      s"value: ${localWeight.values.take(10).mkString(" ")}")
    println(s"inception: ${localWeight(0)}")
  }
}

case class SparsePSCost(
    trainData: RDD[(OneHotVector, Double)],
    bcPart2FeatCountMap: Broadcast[Map[Int, Int]],
    sampleNum: Long,
    sampleHitRate: Double,
    l1reg: Double,
    l2reg: Double,
    truncThreshold: Double) extends DiffFunction[BreezePSVector] {

  def trunc(x: Double, kappa: Double): Double = {
    Math.max(0, x - kappa) - Math.max(0, -x - kappa)
  }

  def truncGradient(gradient: Long2DoubleOpenHashMap, instanceNum: Int, threshold: Double): Int = {
    val sketch = DoublesSketch.builder.build
    val valueIter = gradient.values().iterator()
    while (valueIter.hasNext) {
      sketch.update(math.abs(valueIter.nextDouble()))
    }

    var truncNum = 0
    val iter = gradient.long2DoubleEntrySet().fastIterator()
    var entry: Long2DoubleMap.Entry = null
    while (iter.hasNext) {
      entry = iter.next()
      val aver = entry.getDoubleValue / instanceNum
      if (trunc(aver, threshold) == 0.0) {
        iter.remove()
        truncNum += 1
      }
    }
    truncNum
  }

  override def calculate(x: BreezePSVector) : (Double, BreezePSVector) = {
    val cumGradient = PSVector.duplicate(x).toBreeze

    val xPS = x.toCache

    println(s"x nnz: ${x.norm(0)}")
    val (cumLoss, selectedNum) = trainData.mapPartitionsWithIndex { case (pId, iter) =>
      var begin = System.currentTimeMillis()
      val localX = xPS.pullFromCache()
      var end = System.currentTimeMillis()
      println(s"x pull from cache time: ${end - begin} ms x nnz: ${localX.toSparse.nnz}")

      begin = end
      val partFeatCount = bcPart2FeatCountMap.value(pId)
      val gradientSum = new Long2DoubleOpenHashMap(partFeatCount)

      var instCount = 0
      val rand = new Random()
      val lossSum = iter.map { case (feat, label) =>
        val hitRate = 1.0 / sampleHitRate
        if (rand.nextDouble() < hitRate) {
          val margin = -1.0 * BLAS.dot(feat, localX)
          val gradientMultiplier = (1.0 / (1.0 + math.exp(margin))) - label
          val loss =
            if (label > 0) {
              // log1p is log(1+p) but more accurate for small p
              math.log1p(math.exp(margin))
            } else {
              math.log1p(math.exp(margin)) - margin
            }
          feat.indices.foreach { index => gradientSum.addTo(index, gradientMultiplier) }
          instCount += 1
          loss
        } else {
          0.0
        }
      }.sum
      end = System.currentTimeMillis()
      println(s"pass data time: ${end - begin} ms")

      begin = end
      val truncNum = truncGradient(gradientSum, instCount, truncThreshold)
      cumGradient.toCache.incrementWithCache(new SparseVector(localX.length, gradientSum))
      end = System.currentTimeMillis()
      println(s"trunc threshold: $truncThreshold trunc num: $truncNum")
      println(s"increment gradient to ps time: ${end - begin} ms gradient size: ${gradientSum.size()}")
      Iterator.single(Tuple2(lossSum, instCount))
    }.reduce { case ((l1, c1), (l2, c2)) => (l1 + l2, c1 + c2)}

    PullMan.release(xPS)
    cumGradient.toCache.flushIncrement()

    println(s"select instance num: $selectedNum")
    var begin = System.currentTimeMillis()
    cumGradient :*= 1.0 / selectedNum
    var end = System.currentTimeMillis()
    println(s"scale gradient nnz: ${cumGradient.norm(0)} time: ${end - begin} ms")

    begin = end
    val l2Loss = if (l2reg != 0) {
      cumGradient += x :* (0.5 * l2reg)
      l2reg * cumGradient.norm(2)
    } else {
      0.0
    }

    val l1Loss = if (l1reg != 0.0) {
      cumGradient.mapInto(new ADMMZUpdater(l1reg))
      cumGradient.toSparse.compress()
      l1reg * cumGradient.norm(1)
    } else {
      0.0
    }

    end = System.currentTimeMillis()
    println(s"deal with reg time: ${end - begin} ms")

    val totalLoss = cumLoss / selectedNum + l2Loss + l1Loss
    println(s"sampleNum: $selectedNum cumLoss: $cumLoss data loss: ${cumLoss / selectedNum} " +
      s"l1 loss: $l1Loss l2 loss: $l2Loss total loss: $totalLoss")

    begin = System.currentTimeMillis()
    println(s"gradient nnz: ${cumGradient.norm(0)}")
    end = System.currentTimeMillis()
    println(s"norm time: ${end - begin} ms")

    val localGrad = cumGradient.pull.toSparse
    println(s"local gradient index: ${localGrad.indices.slice(0, 10).mkString(" ")} " +
      s"value: ${localGrad.values.slice(0, 10).mkString(" ")}")
    (totalLoss, cumGradient)
  }
}