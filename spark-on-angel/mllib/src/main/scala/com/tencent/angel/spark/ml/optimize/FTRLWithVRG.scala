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

import com.tencent.angel.spark.linalg.SparseVector
import com.tencent.angel.spark.ml.online_learning.FTRLLearner
import com.tencent.angel.spark.models.vector.SparsePSVector

object FTRLWithVRG {

  def updateZN(zPS: SparsePSVector,
               nPS: SparsePSVector,
               vPS: SparsePSVector,
               dim: Long,
               newWeight: Map[Long, Double],
               alpha: Double,
               beta: Double,
               lambda1: Double,
               lambda2: Double,
               rho1: Double,
               rho2: Double,
               newGrad: Map[Long, Double],
               localGrad: Map[Long, Double],
               aveCoff: Double) = {

    val localV = FTRLLearner.sparsePSModel(vPS)
    val updatedLocalV = computeV(localV, localGrad, rho2, aveCoff)
    vPS.push(new SparseVector(dim, updatedLocalV.toArray))

    val localG = computeG(newGrad, localGrad, updatedLocalV, aveCoff)

    val localD = computeD(FTRLLearner.sparsePSModel(nPS), localG, alpha)
    val incZ = computeIncZ(localG, localD, newWeight)
    val incN = localG.map{case(id, gVal) => (id, gVal * gVal)}

    zPS.increment(new SparseVector(dim, incZ.toArray))
    nPS.increment(new SparseVector(dim, incN.toArray))

  }

  def getNewWeight(zPS: SparsePSVector,
                   nPS: SparsePSVector,
                   localW: Map[Long, Double],
                   rho1: Double,
                   alpha: Double,
                   beta: Double,
                   lambda1: Double,
                   lambda2: Double): (Map[Long, Double], Map[Long, Double]) = {

    val localZ = FTRLLearner.sparsePSModel(zPS)
    val localN = FTRLLearner.sparsePSModel(nPS)
    val newWeight = FTRLLearner.getNewWeight(localZ, localN, alpha, beta, lambda1, lambda2)

    val movAveWeight = moveAverage(localW, newWeight, rho1)
    (newWeight, movAveWeight)
  }

  // the moving for average
  def moveAverage(localVec: Map[Long, Double],
                  newVec: Map[Long, Double],
                  k: Double): Map[Long, Double] = {
    incrementVector(kMulVec(k, localVec), kMulVec(1.0 - k, newVec))
  }

  def kMulVec(k: Double, vec: Map[Long, Double]): Map[Long, Double] = {
    vec.map { case (id, value) => (id, k * value) }
  }

  // add updateInc to incrementedVec
  def incrementVector(incrementedVec: Map[Long, Double],
                      updateInc: Map[Long, Double]): Map[Long, Double] = {

    var vecAddResult: Map[Long, Double] = incrementedVec

    updateInc.foreach { case (fId, fInc) =>
      val oriVal = vecAddResult.getOrElse(fId, 0.0)
      val newVal = oriVal + fInc
      vecAddResult += (fId -> newVal)
    }

    vecAddResult
  }

  def computeD(localN: Map[Long, Double],
               localG: Map[Long, Double],
               alpha: Double): Map[Long, Double] = {

    val nSet = localN.keySet
    val gSet = localG.keySet
    (nSet ++ gSet).map{ id =>

      val nVal = localN.getOrElse(id, 0.0)
      val gVal = localG.getOrElse(id, 0.0)
      val dVal = 1.0 / alpha * (Math.sqrt(nVal + gVal * gVal) - Math.sqrt(nVal))
      (id, dVal)
    }.toMap
  }

  def computeIncZ(localG: Map[Long, Double],
                  localD: Map[Long, Double],
                  newWeight: Map[Long, Double]
                 ): Map[Long, Double] = {

    val gSet = localG.keySet
    val dSet = localD.keySet

    (gSet ++ dSet).map{ id =>

      val gVal = localG.getOrElse(id, 0.0)
      val dVal = localD.getOrElse(id, 0.0)
      val wVal = newWeight.getOrElse(id, 0.0)
      (id, gVal - dVal * wVal)
    }.toMap
  }

  def computeG(newGrad: Map[Long, Double],
               localGrad: Map[Long, Double],
               localV: Map[Long, Double],
               aveCoff: Double
              ): Map[Long, Double] = {

    val newGSet = newGrad.keySet
    val localGSet = localGrad.keySet
    val vSet = localV.keySet

    (newGSet ++ localGSet ++ vSet).map{ id =>
      val newGVal = newGrad.getOrElse(id, 0.0) * aveCoff
      val localGVal = localGrad.getOrElse(id, 0.0) * aveCoff
      val vVal = localV.getOrElse(id, 0.0)

      (id, newGVal - localGVal + vVal)
    }.toMap

  }

  def computeV(localV: Map[Long, Double],
               localGrad: Map[Long, Double],
               rho2: Double,
               aveCoff: Double
              ): Map[Long, Double] = {
    incrementVector(kMulVec(rho2, localV), kMulVec((1.0 - rho2) * aveCoff, localGrad))
  }

}