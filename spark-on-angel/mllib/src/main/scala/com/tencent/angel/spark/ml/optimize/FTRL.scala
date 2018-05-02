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

import com.tencent.angel.spark.linalg.{OneHotVector, SparseVector, Vector}
import com.tencent.angel.spark.ml.psf.FTRLWUpdater
import com.tencent.angel.spark.models.vector.{PSVector, SparsePSVector}

class FTRL(lambda1: Double, lambda2: Double, alpha: Double, beta: Double, regularSkipFeatIndex: Long = 0) extends Serializable {

  var zPS: SparsePSVector = null
  var nPS: SparsePSVector = null

  def initPSModel(dim: Long): Unit = {
    zPS = PSVector.longKeySparse(dim, -1, 5)
    nPS = PSVector.duplicate(zPS)
  }

  def optimize(
      batch: Array[(Vector, Double)],
      costFun: (SparseVector, Double, Vector) => (SparseVector, Double)): Double = {

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

    val deltaZ = new SparseVector(dim, featIds.length)
    val deltaN = new SparseVector(dim, featIds.length)

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
      localZ: SparseVector,
      localN: SparseVector,
      costFun: (SparseVector, Double, Vector) => (SparseVector, Double)
  ): (SparseVector, SparseVector, Double) = {

    val featIndices = feature match {
      case sv: SparseVector => sv.indices
      case ov: OneHotVector => ov.indices
      case _ => throw new Exception("only support SparseVector and OneHotVector")
    }

    val wPairs = featIndices.map { fId =>
      val zVal = localZ(fId)
      val nVal = localN(fId)

      val wVal = updateWeight(fId, zVal, nVal, alpha, beta, lambda1, lambda2)
      (fId, wVal)
    }
    val localW = new SparseVector(feature.length, wPairs)


    val (newGradient, loss) = costFun(localW, label, feature)

    val deltaZ = new SparseVector(feature.length)
    val deltaN = new SparseVector(feature.length)

    featIndices.foreach { fId =>
      val nVal = localN(fId)
      val gOnId = newGradient(fId)
      val dOnId = 1.0 / alpha * (Math.sqrt(nVal + gOnId * gOnId) - Math.sqrt(nVal))

      deltaZ.put(fId, gOnId - dOnId * localW(fId))
      deltaN.put(fId, gOnId * gOnId)
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
  private def plusTo(a: SparseVector, b: SparseVector): Unit = {
    val iter = a.keyValues.long2DoubleEntrySet().fastIterator()
    var entry: Long2DoubleMap.Entry = null
    while(iter.hasNext) {
      entry = iter.next()
      b.keyValues.addTo(entry.getLongKey, entry.getDoubleValue)
    }
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