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

package com.tencent.angel.spark.examples.util

import com.tencent.angel.spark.models.vector.enhanced.{BreezePSVector, CachedPSVector}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.DiffFunction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import com.tencent.angel.spark.models.vector.PSVector
import com.tencent.angel.spark.linalg.{DenseVector => SONADV}

/**
 * This is LogisticRegression data generator and its DiffFunction.
 * We have implemented three styles of DiffFunction:
 * 1. Pure Spark Style, without PS
 * 2. PS mode, update PSVector by RemotePSVector.increment
 * 3. PS mode, update PSVector by RDDPSFunction.psAggregate
 */
object Logistic {

  def generateLRData(sampleNum: Int, dim: Int, partitionNum: Int): RDD[(Vector, Double)] = {
    val spark = SparkSession.builder().getOrCreate()
    val rand = new Random(42)

    val initWeight = new DenseVector((0 until dim).map(_ => rand.nextGaussian()).toArray)
    val bcWeight = spark.sparkContext.broadcast(initWeight)

    spark.sparkContext.parallelize(0 until partitionNum, partitionNum)
      .flatMap { pid =>
        val rand = new Random(42 + pid)
        (0 until (sampleNum / partitionNum)).map { instanceId =>
          val featArray = (0 until dim).toArray.map(_ => rand.nextGaussian())

          val feature = new DenseVector(featArray)

          val score = (0 until dim).map(i => feature(i) * bcWeight.value(i)).sum
          val prob = 1.0 / (1.0 + math.exp(-1 * score))

          val label = if (rand.nextInt() < prob) 1.0 else 0.0
          (feature, label)
        }
      }
  }

  case class Cost(trainData: RDD[(Vector, Double)]) extends DiffFunction[BDV[Double]] {
    def calculate(x: BDV[Double]): (Double, BDV[Double]) = {
      val sampleNum = trainData.count()

      val (cumGradient, cumLoss) = {
        val seqOp = (c: (BDV[Double], Double), point: (Vector, Double)) => {
          val (feat, label) = point
          val (combGrad, combLoss) = c
          val brzData = new BDV[Double](feat.toArray)
          val margin: Double = -1.0 * x.dot(brzData)
          val gradientMultiplier = (1.0 / (1.0 + math.exp(margin))) - label
          val gradient = brzData * gradientMultiplier
          val loss =
            if (label > 0) {
              // log1p is log(1+p) but more accurate for small p
              math.log1p(math.exp(margin))
            } else {
              math.log1p(math.exp(margin)) - margin
            }
          combGrad += gradient
          (combGrad, combLoss + loss)
        }
        val combOp = (c1: (BDV[Double], Double), c2: (BDV[Double], Double)) => {
          (c1._1 + c2._1, c1._2 + c2._2)
        }
        trainData.treeAggregate((new BDV[Double](x.length), 0.0)) (seqOp, combOp)
      }
      val resGradient = new BDV[Double](cumGradient.toArray.map(_ / sampleNum))
      (cumLoss / sampleNum, resGradient)
    }
  }


  case class PSCost(trainData: RDD[(Vector, Double)]) extends DiffFunction[BreezePSVector] {

    override def calculate(x: BreezePSVector) : (Double, BreezePSVector) = {
      val localX = new BDV[Double](x.pull.toDense.values)
      val bcX = trainData.sparkContext.broadcast(localX)
      val cumGradient = PSVector.duplicate(x.component).toBreeze

      val sampleNum = trainData.count()
      val cumLoss = trainData.mapPartitions { iter =>
        val lossArray = new ArrayBuffer[Double]()
        val gradientSum = iter.map { case (feat, label) =>
          val brzData = new BDV[Double](feat.toArray)
          val margin: Double = -1.0 * bcX.value.dot(brzData)
          val gradientMultiplier = (1.0 / (1.0 + math.exp(margin))) - label
          val gradient = brzData * gradientMultiplier
          val loss =
            if (label > 0) {
              // log1p is log(1+p) but more accurate for small p
              math.log1p(math.exp(margin))
            } else {
              math.log1p(math.exp(margin)) - margin
            }
          lossArray += loss
          gradient
        }.reduce(_ + _)
        cumGradient.toCache.increment(new SONADV(gradientSum.toArray))
        lossArray.toIterator
      }.sum()

      BreezePSVector.blas.scal(1.0 / sampleNum, cumGradient)
      (cumLoss / sampleNum, cumGradient)
    }
  }

  case class PSAggregateCost(trainData: RDD[(Vector, Double)])
    extends DiffFunction[BreezePSVector] {

    case class Aggregator(bcX: Broadcast[BDV[Double]], remoteGradient: CachedPSVector)
      extends Serializable {
      private var lossSum = 0.0
      private var count = 0L
      def add(point: (Vector, Double)): Aggregator = {
        val brzFeat = new BDV[Double](point._1.toArray)
        val label = point._2
        val margin = -1.0 * bcX.value.dot(brzFeat)
        val multiplier = (1.0 / (1.0 + math.exp(margin))) - label
        val gradient = brzFeat * multiplier
        val loss = if (label > 0) {
          math.log1p(math.exp(margin))
        } else {
          math.log1p(math.exp(margin)) - margin
        }

        remoteGradient.incrementWithCache(new SONADV(gradient.toArray))
        lossSum += loss
        count += 1
        this
      }

      def merge(other: Aggregator): Aggregator = {
        this.lossSum += other.lossSum
        this.count += other.count
        this
      }

      def loss: Double = lossSum / count

      def gradient: BreezePSVector = {
        remoteGradient.toBreeze :* (1.0 / count)
      }
    }

    override def calculate(x: BreezePSVector): (Double, BreezePSVector) = {
      import com.tencent.angel.spark.rdd.RDDPSFunctions._

      val localX = new BDV(x.pull.toDense.values)
      val bcX = trainData.sparkContext.broadcast(localX)
      val cumGradient = PSVector.duplicate(x.component).toCache

      val aggregator = {
        val seqOp = (c: Aggregator, point: (Vector, Double)) => c.add(point)
        val combOp = (c1: Aggregator, c2: Aggregator) => c1.merge(c2)
        trainData.psAggregate(Aggregator(bcX, cumGradient))(seqOp, combOp)
      }
      (aggregator.loss, aggregator.gradient)
    }
  }
}
