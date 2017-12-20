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
 *
 */

package com.tencent.angel.ml.optimizer.ftrl

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.{TVector, VectorType}
import com.tencent.angel.ml.math.vector._
import com.tencent.angel.ml.model.PSModel
import com.tencent.angel.ml.optimizer.sgd.loss.Loss
import com.tencent.angel.worker.storage.DataBlock
import org.apache.commons.logging.LogFactory

import scala.reflect.runtime.universe._


/**
  * A FTRL Optimizer, according to "Ad click prediction: a view from the trenches"
  */

object FTRL {
  val LOG = LogFactory.getLog(FTRL.getClass)


  def ftrl(zMat: PSModel,
           nMat: PSModel,
           loss: Loss,
           trainData: DataBlock[LabeledData],
           batchSize: Int,
           alpha: Double,
           beta: Double,
           lambda1: Double,
           lambda2: Double): TDoubleVector = {

    // z^t=\Sigma_{s=1}^t g - \Sigma_{s=1}^t \delta^s \cdot w
    val z = zMat.getRow(0).asInstanceOf[TDoubleVector]
    val zUpdate = z.clone

    // n^t = \Sigma_{i=1}^t g_i^2
    val n = nMat.getRow(0).asInstanceOf[TDoubleVector]
    val nUpdate = n.clone

    val w = z match {
      case _: DenseDoubleVector => new DenseDoubleVector(z.getDimension)
      case _: SparseDoubleVector => new SparseDoubleVector(z.getDimension)
      case _: SparseLongKeyDoubleVector => new SparseLongKeyDoubleVector(z.getDimension)
    }

    var totalLoss = 0.0
    val batchNum = trainData.size() / batchSize
    for (batch <- 0 until batchNum) {
      var batchLoss = 0.0

      // Compute w_t
      computeW(z, n, w, alpha, beta, lambda1, lambda2)

      for (samp <- 0 until batchSize) {
        val data = trainData.loopingRead()

        // Predict and compute gradient scaler
        val pre = w.dot(data.getX)
        val gradScaler = -loss.grad(pre, data.getY)
        batchLoss += loss.loss(pre, data.getY)

        // Compute z and n
        computeZandN(z, n, w, data, gradScaler, alpha, beta, lambda1, lambda2)
      }
    }

    LOG.info(s"ftrl train loss = $totalLoss")
    zUpdate.plusBy(z, -1.0)
    nUpdate.plusBy(n, -1.0)

    zMat.increment(zUpdate.timesBy(-1.0))
    nMat.increment(nUpdate.timesBy(-1.0))

    zMat.syncClock()
    nMat.syncClock()

    w
  }


  /**
    * Compute w_t
    *
    * @param z
    * @param w
    * @param alpha
    * @param beta
    * @param lambda1
    * @param lambda2
    */
  def computeW(z: TDoubleVector,
               n: TDoubleVector,
               w: TDoubleVector,
               alpha: Double,
               beta: Double,
               lambda1: Double,
               lambda2: Double): Unit = {
    /*
    w_i^{t+1}
    \begin{cases}
    & 0, \text{ if } |z_i^t|<\lambda_1
    & -(\lambda_2+\Sigma_{s=1}^t \delta^s)^{-1}(z_i^t-\lambda_1)sgn(z_i^t) )),
    \text{otherwise }
    \end{cases}
    */

    z match {
      case _: DenseDoubleVector =>
        for (i <- 0 until w.getDimension) {
          if (z.get(i) < lambda1 && z.get(i) > -lambda1) {
            w.set(i, 0)
          }
          else {
            w.set(i, -(z.get(i) - Math.signum(z.get(i)) * lambda1) /
              (((beta + Math.sqrt(n.get(i))) / alpha) + lambda2))
          }
        }

      case vector: SparseDoubleVector =>
        w.clear()
        val iter = vector
          .getIndexToValueMap
          .int2DoubleEntrySet
          .fastIterator

        while (iter.hasNext) {
          val entry = iter.next()
          val i = entry.getIntKey
          val z_i = entry.getDoubleValue

          if (z_i < lambda1 && z_i > -lambda1) {
            w.set(i, 0)
          }

          else {
            w.set(i, -(z_i - Math.signum(z_i) * lambda1) /
              (((beta + Math.sqrt(n.get(i))) / alpha) + lambda2))
          }
        }

      case vector: SparseLongKeyDoubleVector =>
        w.clear()
        val iter = vector.getIndexToValueMap.long2DoubleEntrySet.fastIterator

        while (iter.hasNext) {
          val entry = iter.next()
          val i = entry.getLongKey
          val z_i = entry.getDoubleValue

          if (z_i < lambda1 && z_i > -lambda1) {
            w.set(i, 0)
          }

          else {
            w.set(i, -(z_i - Math.signum(z_i) * lambda1) /
              (((beta + Math.sqrt(n.get(i))) / alpha) + lambda2))
          }
        }
    }
  }

  /**
    * Compute z_t and n_t
    *
    * @param z
    * @param n
    * @param w
    * @param data
    * @param gradScaler
    * @param alpha
    * @param beta
    * @param lambda1
    * @param lambda2
    */
  def computeZandN(z: TDoubleVector,
                   n: TDoubleVector,
                   w: TDoubleVector,
                   data: LabeledData,
                   gradScaler: Double,
                   alpha: Double,
                   beta: Double,
                   lambda1: Double,
                   lambda2: Double): Unit = {

    data.getX match {

      case sparseSorted: SparseDoubleSortedVector => {
        // Receive feature vector x_t and index vector idx_t
        val feature = data.getX.asInstanceOf[SparseDoubleSortedVector].getValues
        val index = data.getX.asInstanceOf[SparseDoubleSortedVector].getIndices

        // z_i \leftarrow z_i + g_i - \delta_iw_{t,i}
        // n_i \leftarrow n_i + g_i^2
        for (i <- 0 until index.length) {
          val idx = index(i)

          // g_i = (pre_t - y_t)*x_i
          val g = gradScaler * feature(i)

          // \delta_i = \frac{1}{\alpha} (\sqrt{n_i + g_i^2} - \sqrt{n_i})
          // equals \frac{1}{\eta_{t,i}} - \frac{1}{\eta_{t-1,i}}
          val delta = alpha * (Math.sqrt(n.get(idx) + g * g) / alpha - Math.sqrt(n.get(idx)))

          // Update z and n
          z.plusBy(idx, g - delta * w.get(idx))
          n.plusBy(idx, g * g)
        }
      }

      case dummy: SparseDummyVector => {
        // Receive feature vector x_t and index vector idx_t
        val index = data.getX.asInstanceOf[SparseDummyVector].getIndices

        // z_i \leftarrow z_i + g_i - \delta_iw_{t,i}
        // n_i \leftarrow n_i + g_i^2
        for (i <- 0 until index.length) {
          val idx = index(i)

          // g_i = (pre_t - y_t)*x_i
          val g = gradScaler

          // \delta_i = \frac{1}{\alpha} (\sqrt{n_i + g_i^2} - \sqrt{n_i})
          // equals \frac{1}{\eta_{t,i}} - \frac{1}{\eta_{t-1,i}}
          val delta = alpha * (Math.sqrt(n.get(idx) + g * g) / alpha - Math.sqrt(n.get(idx)))

          // Update z and n
          z.plusBy(idx, g - delta * w.get(idx))
          n.plusBy(idx, g * g)
        }
      }
    }
  }
}
