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
import com.tencent.angel.spark.models.vector.enhanced.BreezePSVector
import org.apache.spark.rdd.RDD

import com.tencent.angel.spark.linalg.{BLAS, DenseVector, OneHotVector}
import com.tencent.angel.spark.models.vector.cache.{PullMan, PushMan}
import com.tencent.angel.spark.linalg.{DenseVector => SONADV}

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
          val margin: Double = -1.0 * BLAS.dot(feat, new SONADV(bcX.value.toArray))
          val gradientMultiplier = (1.0 / (1.0 + math.exp(margin))) - label
          val loss =
            if (label > 0) {
              // log1p is log(1+p) but more accurate for small p
              math.log1p(math.exp(margin))
            } else {
              math.log1p(math.exp(margin)) - margin
            }

          // combGrad += gradientMultiplier * feature
          feat.indices.foreach { index => combGrad(index.toInt) += gradientMultiplier }
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

      val localX = xPS.pullFromCache().toDense.values
      println(s"local x sum: ${localX.sum} 10 element: ${localX.slice(0, 9).mkString(" ")}")

      val cumLoss = trainData.mapPartitions { iter =>
        println(s"pull man size: ${PullMan.cacheSize} push man size: ${PushMan.cacheSize}")
        val localX = xPS.pullFromCache()
        val gradientSum = new Array[Double](localX.length.toInt)

        val lossSum = iter.map { case (feat, label) =>
          val margin: Double = -1.0 * BLAS.dot(feat, localX)
          val gradientMultiplier = (1.0 / (1.0 + math.exp(margin))) - label
          val loss =
            if (label > 0) {
              // log1p is log(1+p) but more accurate for small p
              math.log1p(math.exp(margin))
            } else {
              math.log1p(math.exp(margin)) - margin
            }
          feat.indices.foreach { index => gradientSum(index.toInt) += gradientMultiplier }
          loss
        }.sum

        cumGradientPS.incrementWithCache(new DenseVector(gradientSum))
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
}
