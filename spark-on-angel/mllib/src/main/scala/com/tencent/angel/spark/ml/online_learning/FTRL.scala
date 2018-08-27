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


package com.tencent.angel.spark.ml.online_learning

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{LongDoubleVector, LongDummyVector, Vector}
import com.tencent.angel.spark.ml.psf.FTRLWUpdater
import com.tencent.angel.spark.models.vector.{PSVector, SparsePSVector}

class FTRL(lambda1: Double, lambda2: Double, alpha: Double, beta: Double, regularSkipFeatIndex: Long = 0) extends Serializable {

  var zPS: SparsePSVector = _
  var nPS: SparsePSVector = _

  def initPSModel(dim: Long): Unit = {
    zPS = PSVector.longKeySparse(dim, -1, 5)
    nPS = PSVector.duplicate(zPS)
  }

  def optimize(batch: Array[(Vector, Double)],
               costFun: (LongDoubleVector, Double, Vector) => (LongDoubleVector, Double)): Double = {

    val dim = batch.head._1.dim()
    val featIds = batch.flatMap { case (v, _) =>
      v match {
        case longV: LongDoubleVector => longV.getStorage.getIndices
        case dummyV: LongDummyVector => dummyV.getIndices
        case _ => throw new Exception("only support SparseVector and DummyVector")
      }
    }.distinct

    val localZ = zPS.pull(featIds).asInstanceOf[LongDoubleVector]
    val localN = nPS.pull(featIds).asInstanceOf[LongDoubleVector]

    val deltaZ = VFactory.sparseLongKeyDoubleVector(dim)
    val deltaN = VFactory.sparseLongKeyDoubleVector(dim)

    val lossSum = batch.map { case (feature, label) =>
      val (littleZ, littleN, loss) = optimize(feature, label, localZ, localN, costFun)
      plusTo(littleZ, deltaZ)
      plusTo(littleN, deltaN)

      loss
    }.sum

    zPS.increment(deltaZ)
    nPS.increment(deltaN)

    lossSum / batch.length
  }

  def optimize(
                feature: Vector,
                label: Double,
                localZ: LongDoubleVector,
                localN: LongDoubleVector,
                costFun: (LongDoubleVector, Double, Vector) => (LongDoubleVector, Double)
              ): (LongDoubleVector, LongDoubleVector, Double) = {

    val featIndices = feature match {
      case longV: LongDoubleVector => longV.getStorage.getIndices
      case dummyV: LongDummyVector => dummyV.getIndices
    }

    val fetaValues = featIndices.map { fId =>
      val zVal = localZ.get(fId)
      val nVal = localN.get(fId)

      updateWeight(fId, zVal, nVal, alpha, beta, lambda1, lambda2)
    }
    val localW = VFactory.sparseLongKeyDoubleVector(feature.dim, featIndices, fetaValues)


    val (newGradient, loss) = costFun(localW, label, feature)

    val deltaZ = VFactory.sparseLongKeyDoubleVector(feature.dim)
    val deltaN = VFactory.sparseLongKeyDoubleVector(feature.dim)

    featIndices.foreach { fId =>
      val nVal = localN.get(fId)
      val gOnId = newGradient.get(fId)
      val dOnId = 1.0 / alpha * (Math.sqrt(nVal + gOnId * gOnId) - Math.sqrt(nVal))

      deltaZ.set(fId, gOnId - dOnId * localW.get(fId))
      deltaN.set(fId, gOnId * gOnId)
    }
    (deltaZ, deltaN, loss)
  }

  def weight: SparsePSVector = {
    val wPS = zPS.toBreeze.zipMapWithIndex(nPS.toBreeze,
      new FTRLWUpdater(alpha, beta, lambda1, lambda2, regularSkipFeatIndex))

    wPS.toSparse.compress()
    wPS.toSparse
  }

  // b will be returned by the way of adding a
  private def plusTo(a: LongDoubleVector, b: LongDoubleVector): Unit = {
    b.iadd(a)
  }

  def updateWeight(
                    fId: Long,
                    zOnId: Double,
                    nOnId: Double,
                    alpha: Double,
                    beta: Double,
                    lambda1: Double,
                    lambda2: Double): Double = {
    if (fId == regularSkipFeatIndex) {
      -1.0 * alpha * zOnId / (beta + Math.sqrt(nOnId))
    } else if (Math.abs(zOnId) <= lambda1) {
      0.0
    } else {
      (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nOnId)) / alpha)) * (zOnId - Math.signum(zOnId).toInt * lambda1)
    }
  }

}
