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

package com.tencent.angel.spark.ml.sparse

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.DiffFunction

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.vector.enhanced.{BreezePSVector, CachedPSVector, PullMan, PushMan}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD

import com.tencent.angel.spark.ml.common.OneHot
import com.tencent.angel.spark.ml.common.OneHot.OneHotVector
import com.tencent.angel.spark.models.vector.{PSVector, VectorType}

/**
 * This is LogisticRegression data generator and its DiffFunction.
 * We have implemented three styles of DiffFunction:
 * 1. Pure Spark Style, without PS
 * 2. PS mode, update PSVector by RemotePSVector.increment
 * 3. PS mode, update PSVector by RDDPSFunction.psAggregate
 */

object SparseLogistic {

  case class Cost(trainData: RDD[(OneHotVector, Double)]) extends DiffFunction[BDV[Double]] {
    def calculate(x: BDV[Double]): (Double, BDV[Double]) = {
      val sampleNum = trainData.count()
      val bcX = trainData.sparkContext.broadcast(x)

      val (cumGradient, cumLoss) = {
        val seqOp = (c: (BDV[Double], Double), point: (OneHotVector, Double)) => {
          val (feat, label) = point
          val (combGrad, combLoss) = c
          val margin: Double = -1.0 * OneHot.dot(feat, bcX.value)
          val gradientMultiplier = (1.0 / (1.0 + math.exp(margin))) - label
          val loss =
            if (label > 0) {
              // log1p is log(1+p) but more accurate for small p
              math.log1p(math.exp(margin))
            } else {
              math.log1p(math.exp(margin)) - margin
            }

          // combGrad += gradientMultiplier * feature
          feat.foreach { index => combGrad(index) += gradientMultiplier }
          (combGrad, combLoss + loss)
        }
        val combOp = (c1: (BDV[Double], Double), c2: (BDV[Double], Double)) => {
          (c1._1 + c2._1, c1._2 + c2._2)
        }
        trainData.treeAggregate((new BDV[Double](x.length), 0.0)) (seqOp, combOp)
      }
      val resGradient = new BDV[Double](cumGradient.toArray.map(_ / sampleNum))
      println(s"loss: ${cumLoss / sampleNum}")
      (cumLoss / sampleNum, resGradient)
    }
  }


  case class PSCost(trainData: RDD[(OneHotVector, Double)]) extends DiffFunction[BreezePSVector] {

    override def calculate(x: BreezePSVector) : (Double, BreezePSVector) = {
      val cumGradient = PSContext.instance().duplicateVector(x.component).toBreeze

      val cumGradientPS = cumGradient.toCache
      val xPS = x.toCache

      val localX = xPS.pullFromCache()
      println(s"local x sum: ${localX.sum} 10 element: ${localX.slice(0, 9).mkString(" ")}")

      val cumLoss = trainData.mapPartitions { iter =>
        println(s"pull man size: ${PullMan.cacheSize} push man size: ${PushMan.cacheSize}")
        val localX = xPS.pullFromCache()
        val gradientSum = new Array[Double](localX.length)

        val lossSum = iter.map { case (feat, label) =>
          val margin: Double = -1.0 * OneHot.dot(feat, localX)
          val gradientMultiplier = (1.0 / (1.0 + math.exp(margin))) - label
          val loss =
            if (label > 0) {
              // log1p is log(1+p) but more accurate for small p
              math.log1p(math.exp(margin))
            } else {
              math.log1p(math.exp(margin)) - margin
            }
          feat.foreach { index => gradientSum(index) += gradientMultiplier }
          loss
        }.sum

        cumGradientPS.incrementWithCache(gradientSum)
        Iterator.single(lossSum)
      }.sum()

      PullMan.release(xPS)
      cumGradientPS.flushIncrement()

      val sampleNum = trainData.count()
      cumGradient :*= 1.0 /sampleNum

      println(s"sampleNum: $sampleNum cumLoss: $cumLoss loss: ${cumLoss / sampleNum}")
      (cumLoss / sampleNum, cumGradient)
    }
  }

  case class PSAggregateCost(trainData: RDD[(Vector, Double)]) extends DiffFunction[BreezePSVector] {

    case class Aggregator(bcX: Broadcast[BDV[Double]], remoteGradient: CachedPSVector) extends Serializable {
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

        remoteGradient.incrementWithCache(gradient.toArray)

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
    }

    override def calculate(x: BreezePSVector): (Double, BreezePSVector) = {
      import com.tencent.angel.spark.rdd.RDDPSFunctions._

      val localX = new BDV(x.pull())
      val bcX = trainData.sparkContext.broadcast(localX)
      val cumGradient = PSVector.duplicate(x.component).toCache

      val aggregator = {
        val seqOp = (c: Aggregator, point: (Vector, Double)) => c.add(point)
        val combOp = (c1: Aggregator, c2: Aggregator) => c1.merge(c2)
        trainData.psAggregate(Aggregator(bcX, cumGradient))(seqOp, combOp)
      }
      (aggregator.loss, cumGradient.toBreeze)
    }
  }
}
