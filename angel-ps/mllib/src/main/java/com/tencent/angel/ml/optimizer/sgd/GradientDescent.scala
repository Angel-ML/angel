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

package com.tencent.angel.ml.optimizer.sgd

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector._
import com.tencent.angel.ml.model.PSModel
import com.tencent.angel.ml.optimizer.sgd.loss.Loss
import com.tencent.angel.worker.storage.DataBlock
import org.apache.commons.logging.{Log, LogFactory}


object GradientDescent {
  private val LOG: Log = LogFactory.getLog(GradientDescent.getClass)

  type baseT = TDoubleVector

  def miniBatchGD(trainData: DataBlock[LabeledData],
                                      wM: PSModel,
                                      intercept: Option[PSModel],
                                      lr: Double,
                                      loss: Loss,
                                      batchSize: Int,
                                      batchNum: Int): (Double, baseT) = {

    //Pull model from PS Server
    val w = wM.getRow(0).asInstanceOf[baseT]
    var b: Option[Double] = intercept.map(_.getRow(0).asInstanceOf[baseT].get(0))
    var totalLoss = 0.0

    val taskContext = wM.getTaskContext

    for (batch <- 1 to batchNum) {
      val batchStartTs = System.currentTimeMillis()

      val grad =
        w match {
          case _: DenseDoubleVector => new DenseDoubleVector(w.getDimension)
          case _: SparseDoubleVector => new SparseDoubleVector(w.getDimension)
          case _: SparseLongKeyDoubleVector => new SparseLongKeyDoubleVector(w.asInstanceOf[SparseLongKeyDoubleVector].getLongDim)
          case _ => new DenseDoubleVector(w.getDimension)
        }

      grad.setRowId(0)

      val bUpdate = new DenseDoubleVector(1)
      bUpdate.setRowId(0)

      var batchLoss: Double = 0.0
      var gradScalarSum: Double = 0.0

      for (i <- 0 until batchSize) {
        val data = trainData.loopingRead()
        val x = data.getX
        val y = data.getY
        val pre = w.dot(x) + b.getOrElse(0.0)
        val gradScalar = -loss.grad(pre, y)    // not negative gradient
        grad.plusBy(x, gradScalar)
        batchLoss += loss.loss(pre, y)
        gradScalarSum += gradScalar
      }

      grad.timesBy(1.toDouble / batchSize.asInstanceOf[Double])
      gradScalarSum /= batchSize

      if (loss.isL2Reg) {
        L2Loss(loss, w, grad)
      }

      if (loss.isL1Reg) {
        truncGradient(grad, 0, loss.getRegParam)
      }

      totalLoss += batchLoss
      w.plusBy(grad, -1.0 * lr)
      b = b.map(bv => bv - lr * gradScalarSum)

      wM.increment(grad.timesBy(-1.0 * lr).asInstanceOf[TDoubleVector])
      intercept.map { bv =>
        bUpdate.set(0, -lr * gradScalarSum)
        bv.increment(bUpdate)
        bv
      }

      LOG.debug(s"Batch[$batch] loss = $batchLoss")
      taskContext.updateProfileCounter(batchSize, (System.currentTimeMillis() - batchStartTs).toInt)
    }

    //Push model update to PS Server
    totalLoss /= (batchNum*batchSize)
    totalLoss += loss.getReg(w.asInstanceOf[TDoubleVector])

    wM.syncClock()
    intercept.map(_.syncClock())


    (totalLoss , w)
  }

  def L2Loss(loss: Loss, w: baseT, grad: baseT): Unit = {
    grad match {

      case dense: DenseDoubleVector =>
        for (i <- 0 until grad.size) {
          if (Math.abs(dense.get(i)) > 10e-7) {
            grad.set(i, grad.get(i) + w.get(i) * loss.getRegParam)
          }
        }

      case sparse: SparseDoubleVector =>
        val map = sparse.asInstanceOf[SparseDoubleVector].getIndexToValueMap
        val iter = map.int2DoubleEntrySet().fastIterator()

        while (iter.hasNext) {
          val entry = iter.next()
          val k = entry.getIntKey
          val v = entry.getDoubleValue

          if (Math.abs(v) > 10e-7) {
            entry.setValue(v + w.get(k) * loss.getRegParam)
          }
        }

      case long: SparseLongKeyDoubleVector =>
        val map = long.asInstanceOf[SparseLongKeyDoubleVector].getIndexToValueMap
        val iter = map.long2DoubleEntrySet().fastIterator()

        while (iter.hasNext) {
          val entry = iter.next()
          val k = entry.getLongKey
          val v = entry.getDoubleValue

          if (Math.abs(v) > 10e-7) {
            entry.setValue(v + w.get(k) * loss.getRegParam)
          }
        }
    }
  }

  /*
     *  T(v, alpha, theta) =
     *  max(0, v-alpha), if v in [0,theta]
     *  min(0, v-alpha), if v in [-theta,0)
     *  v, otherwise
     *
     *  this function returns the update to the vector
   */
  def truncGradient(vec: baseT, alpha: Double, theta: Double): baseT = {
    val update = new SparseDoubleVector(vec.getDimension)
    for (dim <- 0 until update.getDimension) {
      val value = vec.get(dim)
      if (value >= 0 && value <= theta) {
        val newValue = if (value - alpha > 0) value - alpha else 0
        vec.set(dim, newValue)
        update.set(dim, newValue - value)
      } else if (value < 0 && value >= -theta) {
        val newValue = if (value - alpha < 0) value - alpha else 0
        vec.set(dim, newValue)
        update.set(dim, newValue - value)
      }
    }

    update
  }

}
