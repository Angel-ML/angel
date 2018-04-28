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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.spark.ml.optimize

import it.unimi.dsi.fastutil.longs.Long2DoubleMap

import com.tencent.angel.spark.linalg.{BLAS, OneHotVector, SparseVector, Vector}
import com.tencent.angel.spark.ml.psf.FTRLWUpdater
import com.tencent.angel.spark.models.vector.{PSVector, SparsePSVector}

class FTRLWithVRG(lambda1: Double,
    lambda2: Double,
    alpha: Double,
    beta: Double,
    rho1: Double,
    rho2: Double,
    regularSkipFeatIndex: Long = 0) extends Serializable {

  var zPS: SparsePSVector = null
  var nPS: SparsePSVector = null
  var vPS: SparsePSVector = null

  def initPSModel(dim: Long): Unit = {
    zPS = PSVector.longKeySparse(dim, -1, 5)
    nPS = PSVector.duplicate(zPS)
    vPS = PSVector.duplicate(zPS)
  }

  def optimize(
      batch: Array[(Vector, Double)],
      localW: SparseVector,
      costFun: (SparseVector, Double, Vector) => (SparseVector, Double)
  ): (SparseVector, Double) = {

    val dim = batch.head._1.length

    val featIds = batch.flatMap { case (feat, label) =>
      feat match {
        case sv: SparseVector => sv.indices
        case ov: OneHotVector => ov.indices
        case _ => throw new Exception("only support SparseVector and OneHotVector")
      }
    }.distinct

    val localZ = zPS.pull(featIds)
    val localN = nPS.pull(featIds)
    val localV = vPS.pull(featIds)

    val wPairs = featIds.map { fId =>
      val zVal = localZ(fId)
      val nVal = localN(fId)

      val wVal = updateWeight(fId, zVal, nVal, alpha, beta, lambda1, lambda2)
      (fId, wVal)
    }
    val newWeight = new SparseVector(dim, wPairs)

    // move averaged weight which will be the current local weight
    val moveAveW = getMoveAveWeight(rho1, localW, newWeight)

    val newGrad = new SparseVector(dim, featIds.length)
    val localGrad = new SparseVector(dim, featIds.length)

    // all data in this partition compute the gradient and finally get the average
    val batchLoss = batch.map{ case(feature, label) =>

      // compute the loss for the new sample by move averaged weight
      val loss = costFun(moveAveW, label, feature)._2

      val newGradient = costFun(newWeight, label, feature)._1
      val localGradient = costFun(moveAveW, label, feature)._1
      plusTo(newGradient, newGrad)
      plusTo(localGradient, localGrad)

      loss
    }.sum

    // get average gradient
    BLAS.scal(1.0 / batch.length, newGrad)
    BLAS.scal(1.0 / batch.length, localGrad)


    // update the z and n model on PS
    val (deltaZ, deltaN) = optimize(localV, localN, newWeight, newGrad, localGrad)

    zPS.increment(deltaZ)
    nPS.increment(deltaN)

    (moveAveW, batchLoss / batch.length)

  }

  def optimize(
      localV: SparseVector,
      localN: SparseVector,
      newWeight: SparseVector,
      newGrad: SparseVector,
      localGrad: SparseVector): (SparseVector, SparseVector) = {

    val updatedV = getMoveAveWeight(rho2, localV, localGrad)
    vPS.push(updatedV)

    val localG = computeG(newGrad, localGrad, updatedV)
    val localD = computeD(localN, localG, alpha)

    val incZ = computeIncZ(localG, localD, newWeight)

    val incN = localG.indices.zip(localG.values).map{case(id, gVal) => (id, gVal * gVal)}

    (incZ, new SparseVector(localN.length, incN))
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
      localM: SparseVector,
      newM: SparseVector): SparseVector = {

    val dim = localM.length
    val localWeighted = localM.indices.zip(localM.values).map{case(id, featVal) => (id, rho * featVal)}
    val newWeighted = newM.indices.zip(newM.values).map{case(id, featVal) => (id, (1.0 - rho) * featVal)}

    val localVector = new SparseVector(dim, localWeighted)
    val newVector = new SparseVector(dim, newWeighted)

    plusTo(newVector, localVector)
    localVector
  }

  // b will be returned by the way of adding a
  private def plusTo(a: SparseVector, b: SparseVector): Unit = {
    val iter = a.keyValues.long2DoubleEntrySet().fastIterator()
    var entry: Long2DoubleMap.Entry = null
    while(iter.hasNext) {
      entry = iter.next()
      b.keyValues.addTo(entry.getLongKey, entry.getDoubleValue)
    }
  }

  def computeD(localN: SparseVector,
      localG: SparseVector,
      alpha: Double): SparseVector = {

    val nSet = localN.indices.toSet
    val gSet = localG.indices.toSet
    val result = (nSet ++ gSet).map{ id =>
      val nVal = localN(id)
      val gVal = localG(id)
      val dVal = 1.0 / alpha * (Math.sqrt(nVal + gVal * gVal) - Math.sqrt(nVal))
      (id, dVal)
    }.toArray

    new SparseVector(localN.length, result)
  }

  def computeIncZ(
      localG: SparseVector,
      localD: SparseVector,
      newWeight: SparseVector): SparseVector = {

    val gSet = localG.indices.toSet
    val dSet = localD.indices.toSet

    val result = (gSet ++ dSet).map{ id =>

      val gVal = localG(id)
      val dVal = localD(id)
      val wVal = newWeight(id)
      (id, gVal - dVal * wVal)
    }.toArray

    new SparseVector(localG.length, result)
  }

  def computeG(
      newGrad: SparseVector,
      localGrad: SparseVector,
      localV: SparseVector): SparseVector = {

    val newGSet = newGrad.indices.toSet
    val localGSet = localGrad.indices.toSet
    val vSet = localV.indices.toSet

    val result = (newGSet ++ localGSet ++ vSet).map{ id =>
      val newGVal = newGrad(id)
      val localGVal = localGrad(id)
      val vVal = localV(id)

      (id, newGVal - localGVal + vVal)
    }.toArray

    new SparseVector(newGrad.length, result)
  }

  def weight: SparsePSVector = {
    val wPS = zPS.toBreeze.zipMapWithIndex(nPS.toBreeze,
      new FTRLWUpdater(alpha, beta, lambda1, lambda2, regularSkipFeatIndex))

    wPS.toSparse.compress()
    wPS.toSparse
  }

}