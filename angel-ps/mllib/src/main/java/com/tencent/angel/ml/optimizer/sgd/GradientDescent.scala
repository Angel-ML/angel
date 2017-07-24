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

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.TAbstractVector
import com.tencent.angel.ml.math.vector._
import com.tencent.angel.ml.model.PSModel
import com.tencent.angel.worker.storage.DataBlock
import org.apache.commons.logging.{Log, LogFactory}

object GradientDescent {
  private val LOG: Log = LogFactory.getLog(GradientDescent.getClass)


  def miniBatchGD[M <: TDoubleVector](trainData: DataBlock[LabeledData],
                                      model: PSModel[M],
                                      intercept: Option[PSModel[M]],
                                      lr: Double,
                                      loss: Loss,
                                      batchSize: Int,
                                      batchNum: Int): (Double, TDoubleVector) = {

    //Pull model from PS Server
    val w = model.getRow(0)
    var b: Option[Double] = intercept.map(_.getRow(0).get(0))
    var totalLoss = 0.0

    val taskContext = model.getTaskContext

    for (batch: Int <- 1 to batchNum) {
      val batchStartTs = System.currentTimeMillis()
      val grad = new DenseDoubleVector(w.getDimension)
      grad.setRowId(0)

      val bUpdate = new DenseDoubleVector(1)
      bUpdate.setRowId(0)

      var batchLoss: Double = 0.0
      var gradScalarSum: Double = 0.0

      for (i <- 0 until batchSize) {
        val (x: TAbstractVector, y: Double) = loopingData(trainData)
        val pre = w.dot(x) + b.getOrElse(0.0)
        val gradScalar = -loss.grad(pre, y)    // not negative gradient
        grad.plusBy(x, gradScalar)
        batchLoss += loss.loss(pre, y)
        gradScalarSum += gradScalar
      }

      grad.timesBy(1.toDouble / batchSize.asInstanceOf[Double])
      gradScalarSum /= batchSize

      if (loss.isL2Reg) {
        for (index <- 0 until grad.size) {
          if (grad.get(index) > 10e-7) {
            grad.set(index, grad.get(index) + w.get(index) * (loss.getRegParam))
          }
        }
      }

      if (loss.isL1Reg) {
        truncGradient(grad, 0, loss.getRegParam)
      }

      totalLoss += batchLoss
      w.plusBy(grad, -1.0 * lr)
      b = b.map(bv => bv - lr * gradScalarSum)

      model.increment(grad.timesBy(-1.0 * lr).asInstanceOf[M])
      intercept.map { bv =>
        bUpdate.set(0, -lr * gradScalarSum)
        bv.increment(bUpdate)
        bv
      }
      LOG.debug(s"Batch[$batch] loss = $batchLoss")
      taskContext.updateProfileCounter(batchSize, (System.currentTimeMillis() - batchStartTs).toInt)
    }

    //Push model update to PS Server
    totalLoss += loss.getReg(w)
    model.clock.get
    intercept.map(_.clock.get)

    (totalLoss, w)
  }

  /**
    * Read LabeledData from DataBlock Looping. If it reach the end, start from the beginning again.
    *
    * @param trainData
    * @return
    */
  def loopingData(trainData: DataBlock[LabeledData]): (TAbstractVector, Double) = {
    var data = trainData.read()
    if (data == null) {
      trainData.resetReadIndex()
      data = trainData.read()
    }

    if (data != null)
      (data.getX, data.getY)
    else
      throw new AngelException("Train data storage is empty or corrupted.")
  }

  def miniBatchGD[M <: TDoubleVector](trainData: DataBlock[LabeledData], model: PSModel[M], lr:
  Double, loss: Loss, batchSize: Int): Double = {

    0.0
  }


  /*
   *  T(v, alpha, theta) =
   *  max(0, v-alpha), if v in [0,theta]
   *  min(0, v-alpha), if v in [-theta,0)
   *  v, otherwise
   *
   *  this function returns the update to the vector
 */
  def truncGradient(vec: TDoubleVector, alpha: Double, theta: Double): TDoubleVector = {
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
