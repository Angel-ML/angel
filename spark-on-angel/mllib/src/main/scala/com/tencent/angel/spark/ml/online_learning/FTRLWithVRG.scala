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
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.vector.{LongDoubleVector, LongDummyVector, Vector}
import com.tencent.angel.spark.ml.psf.FTRLWUpdater
import com.tencent.angel.spark.models.vector.{PSVector, SparsePSVector}

class FTRLWithVRG(lambda1: Double,
                  lambda2: Double,
                  alpha: Double,
                  beta: Double,
                  rho1: Double,
                  rho2: Double,
                  regularSkipFeatIndex: Long = 0) extends Serializable {

  var zPS: SparsePSVector = _
  var nPS: SparsePSVector = _
  var vPS: SparsePSVector = _

  def initPSModel(dim: Long): Unit = {
    zPS = PSVector.longKeySparse(dim, -1, 5)
    nPS = PSVector.duplicate(zPS)
    vPS = PSVector.duplicate(zPS)
  }

  def optimize(
                batch: Array[(Vector, Double)],
                localW: LongDoubleVector,
                costFun: (LongDoubleVector, Double, Vector) => (LongDoubleVector, Double)): (LongDoubleVector, Double) = {

    val dim = batch.head._1.dim

    //@todo: fix OneHotVector
    val featIds = batch.flatMap { case (feat, _) =>
      feat match {
        case longV: LongDoubleVector => longV.getStorage.getIndices
        case dummy: LongDummyVector => dummy.getIndices
        case _ => throw new Exception("only support SparseVector and DummyVector")
      }
    }.distinct

    val localZ = zPS.pull(featIds).asInstanceOf[LongDoubleVector]
    val localN = nPS.pull(featIds).asInstanceOf[LongDoubleVector]
    val localV = vPS.pull(featIds).asInstanceOf[LongDoubleVector]

    val wPairs = featIds.map { fId =>
      val zVal = localZ.get(fId)
      val nVal = localN.get(fId)

      val wVal = updateWeight(fId, zVal, nVal, alpha, beta, lambda1, lambda2)
      (fId, wVal)
    }.unzip

    val newWeight = VFactory.sparseLongKeyDoubleVector(dim, wPairs._1, wPairs._2)

    // move averaged weight which will be the current local weight
    val moveAveW = getMoveAveWeight(rho1, localW, newWeight)

    val newGrad = VFactory.sparseLongKeyDoubleVector(dim, featIds.length)
    val localGrad = VFactory.sparseLongKeyDoubleVector(dim, featIds.length)

    // all data in this partition compute the gradient and finally get the average
    val batchLoss = batch.map { case (feature, label) =>

      // compute the loss for the new sample by move averaged weight
      val loss = costFun(moveAveW, label, feature)._2

      val newGradient = costFun(newWeight, label, feature)._1
      val localGradient = costFun(moveAveW, label, feature)._1
      newGrad.iadd(newGradient)
      localGrad.iadd(localGradient)

      loss
    }.sum

    // get average gradient
    newGrad.imul(1.0 / batch.length)
    localGrad.imul(1.0 / batch.length)

    // update the z and n model on PS
    val (deltaZ, deltaN) = optimize(localV, localN, newWeight, newGrad, localGrad)

    zPS.increment(deltaZ)
    nPS.increment(deltaN)

    (moveAveW, batchLoss / batch.length)

  }

  def optimize(
                localV: LongDoubleVector,
                localN: LongDoubleVector,
                newWeight: LongDoubleVector,
                newGrad: LongDoubleVector,
                localGrad: LongDoubleVector): (LongDoubleVector, LongDoubleVector) = {

    val updatedV = getMoveAveWeight(rho2, localV, localGrad)
    vPS.push(updatedV)

    val localG = computeG(newGrad, localGrad, updatedV)
    val localD = computeD(localN, localG, alpha)

    val incZ = computeIncZ(localG, localD, newWeight)

    val incN = localG.mul(localG).asInstanceOf[LongDoubleVector]

    (incZ, incN)
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

  // get the move average weight
  def getMoveAveWeight(
                        rho: Double,
                        localM: LongDoubleVector,
                        newM: LongDoubleVector): LongDoubleVector = {
    localM.mul(rho).iadd(newM.mul(1.0 - rho)).asInstanceOf[LongDoubleVector]
  }

  def computeD(localN: LongDoubleVector,
               localG: LongDoubleVector,
               alpha: Double): LongDoubleVector = {

    /*
    val nSet = localN.getStorage.getIndices.toSet
    val gSet = localG.getStorage.getIndices.toSet
    val indexes = (nSet ++ gSet).toArray
    val values = indexes.map{ id =>
      val nVal = localN.get(id)
      val gVal = localG.get(id)
      1.0 / alpha * (Math.sqrt(nVal + gVal * gVal) - Math.sqrt(nVal))
    }
    VFactory.sparseLongKeyDoubleVector(localN.getDim, indexes, values)
    */

    val dVector = Ufuncs.sqrt(localN.add(localG.mul(localG))).sub(Ufuncs.sqrt(localN))
    dVector.imul(1.0 / alpha).asInstanceOf[LongDoubleVector]
  }

  def computeIncZ(
                   localG: LongDoubleVector,
                   localD: LongDoubleVector,
                   newWeight: LongDoubleVector): LongDoubleVector = {

    localG.sub(localD.mul(newWeight)).asInstanceOf[LongDoubleVector]
  }

  def computeG(
                newGrad: LongDoubleVector,
                localGrad: LongDoubleVector,
                localV: LongDoubleVector): LongDoubleVector = {
    newGrad.sub(localGrad).add(localV).asInstanceOf[LongDoubleVector]
  }

  def weight: SparsePSVector = {
    val wPS = zPS.toBreeze.zipMapWithIndex(nPS.toBreeze,
      new FTRLWUpdater(alpha, beta, lambda1, lambda2, regularSkipFeatIndex))

    wPS.toSparse.compress()
  }

}
