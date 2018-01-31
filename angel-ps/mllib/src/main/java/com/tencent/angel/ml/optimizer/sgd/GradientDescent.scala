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
import com.tencent.angel.ml.math.vector.{TDoubleVector, _}
import com.tencent.angel.ml.model.PSModel
import com.tencent.angel.ml.optimizer.sgd.loss.Loss
import com.tencent.angel.worker.storage.DataBlock
import org.apache.commons.logging.{Log, LogFactory}

import scala.math.Numeric
import scala.reflect.runtime.universe._

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
    miniBatchGD(trainData, wM, intercept, lr, loss, batchSize, batchNum, new Array[Int](0))
  }

  def miniBatchGD[N: Numeric : TypeTag](trainData: DataBlock[LabeledData],
                                        wM: PSModel,
                                        intercept: Option[PSModel],
                                        lr: Double,
                                        loss: Loss,
                                        batchSize: Int,
                                        batchNum: Int,
                                        indexes : Array[N]): (Double, baseT) = {

    // Pull model from PS Server
    val elementType = typeOf[N]

    val w = if(indexes == null || indexes.length == 0) {
      wM.getRow(0).asInstanceOf[baseT]
    } else {
      elementType match {
        case t if t == typeOf[Int] => wM.getRowWithIndex(0, indexes.asInstanceOf[Array[Int]]).asInstanceOf[TIntDoubleVector]
        case t if t == typeOf[Long] => wM.getRowWithLongIndex(0, indexes.asInstanceOf[Array[Long]]).asInstanceOf[TLongDoubleVector]
        case _ => throw new AngelException(s"unsupported type: $elementType")
      }
    }

    var b: Option[Double] = intercept.map(_.getRow(0).asInstanceOf[baseT].get(0))
    var totalLoss = 0.0

    val taskContext = wM.getTaskContext

    for (batch <- 1 to batchNum) {
      val batchStartTs = System.currentTimeMillis()

      val grad = w match {
        case _: DenseDoubleVector => new DenseDoubleVector(w.getDimension)
        case _: SparseDoubleVector => new SparseDoubleVector(w.getDimension, w.asInstanceOf[TIntDoubleVector].size())
        case _: CompSparseDoubleVector => w.asInstanceOf[CompSparseDoubleVector].cloneAndReset()
        case _: CompSparseLongKeyDoubleVector => w.asInstanceOf[CompSparseLongKeyDoubleVector].cloneAndReset()
        case _: SparseLongKeyDoubleVector => new SparseLongKeyDoubleVector(w.asInstanceOf[TLongDoubleVector].getLongDim, w.asInstanceOf[SparseLongKeyDoubleVector].size())
        case _ => new SparseLongKeyDoubleVector(w.asInstanceOf[TLongDoubleVector].getLongDim, w.asInstanceOf[SparseLongKeyDoubleVector].size())
      }

      grad.setRowId(0)

      val bUpdate = new DenseDoubleVector(1)
      bUpdate.setRowId(0)

      var batchLoss: Double = 0.0
      var gradScalarSum: Double = 0.0

      // 统计 minibatch,有偏置的情况下，w:(w0,w1,w2,...,wn),x:(1,x1,x2,...,xn)
      for (i <- 0 until batchSize) {
        val data = trainData.loopingRead()
        val x = data.getX
        val y = data.getY
        val pre = w.dot(x) + b.getOrElse(0.0)
        val gradScalar = -loss.grad(pre, y)    // not negative gradient
        grad.plusBy(x, gradScalar)
        batchLoss += loss.loss(pre, y)
        // 为偏置求的梯度
        gradScalarSum += gradScalar
      }

      grad.timesBy(1.toDouble / batchSize.asInstanceOf[Double])
      gradScalarSum /= batchSize

      if (loss.isL2Reg) {
        LOG.info("this is in l2")
        L2Loss(loss, w, grad)
      }

      // L is (0,1)
      if(loss.isL1Reg){
        PGD4Grad(w, grad, loss.getRegParam, lr)
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
    totalLoss /= (batchNum * batchSize)
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

      case compSparse: CompSparseDoubleVector => {
        class L2UpdateParam(lossRegParam:Double, w: baseT) extends ElemUpdateParam {
          def getW = w
          def getLossRegParam = lossRegParam
        }

        class L2Updater extends IntDoubleElemUpdater {
          override def action(index: Int, value: Double, param: ElemUpdateParam): Double = {
            value + param.asInstanceOf[L2UpdateParam].getW.get(index) * param.asInstanceOf[L2UpdateParam].getLossRegParam
          }
        }
        compSparse.elemUpdate(new L2Updater, new L2UpdateParam(loss.getRegParam, w))
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

      case compLong : CompSparseLongKeyDoubleVector => {
        class L2UpdateParam(lossRegParam:Double, w: baseT) extends ElemUpdateParam {
          def getW = w
          def getLossRegParam = lossRegParam
        }

        class L2Updater extends LongDoubleElemUpdater {
          override def action(index: Long, value: Double, param: ElemUpdateParam): Double = {
            value + param.asInstanceOf[L2UpdateParam].getW.get(index) * param.asInstanceOf[L2UpdateParam].getLossRegParam
          }
        }
        compLong.elemUpdate(new L2Updater, new L2UpdateParam(loss.getRegParam, w))
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

  // pgd algorithm,just return the gradient and L > 0
  // x(k+1) = x(k) - L * Grad(k)
  // Grad(k) = (x(k) - prox(x - L * grad(x(k)))) / L
  def PGD4Grad(weight: baseT, grad: baseT, l1Reg: Double, L: Double) = {

    val theta = l1Reg * L

    grad match {

      case denseG: DenseDoubleVector =>
        LOG.info("this is dense vector")
        for (i <- 0 until denseG.size) {
          grad.set(i, pdgGetG(weight.get(i), grad.get(i), L, theta))
        }

      case sparseLongG: SparseLongKeyDoubleVector =>
        val iterLongG = grad.asInstanceOf[SparseLongKeyDoubleVector]
          .getIndexToValueMap
          .long2DoubleEntrySet
          .fastIterator

        while (iterLongG.hasNext) {
          val entry = iterLongG.next()
          entry.setValue(pdgGetG(weight.get(entry.getLongKey), entry.getDoubleValue, L, theta))
        }

      case sparseG: SparseDoubleVector =>
        val iterG = grad.asInstanceOf[SparseDoubleVector]
          .getIndexToValueMap
          .int2DoubleEntrySet
          .fastIterator

        while (iterG.hasNext) {
          val entry = iterG.next()
          entry.setValue(pdgGetG(weight.get(entry.getIntKey), entry.getDoubleValue, L, theta))
        }

      case compSparse: CompSparseDoubleVector => {
        class L1UpdateParam(theta:Double, L:Double, w: baseT) extends ElemUpdateParam {
          def getW = w
          def getTheta = theta
          def getL = L
        }

        class L1Updater extends IntDoubleElemUpdater {
          override def action(index: Int, value: Double, param: ElemUpdateParam): Double = {
            pdgGetG(param.asInstanceOf[L1UpdateParam].getW.get(index), value, param.asInstanceOf[L1UpdateParam].getL, param.asInstanceOf[L1UpdateParam].getTheta)
          }
        }

        compSparse.elemUpdate(new L1Updater, new L1UpdateParam(theta, L, weight))
      }

      case compLong : CompSparseLongKeyDoubleVector => {
        class L1UpdateParam(theta:Double, L:Double, w: baseT) extends ElemUpdateParam {
          def getW = w
          def getTheta = theta
          def getL = L
        }

        class L1Updater extends LongDoubleElemUpdater {
          override def action(index: Long, value: Double, param: ElemUpdateParam): Double = {
            pdgGetG(param.asInstanceOf[L1UpdateParam].getW.get(index), value, param.asInstanceOf[L1UpdateParam].getL, param.asInstanceOf[L1UpdateParam].getTheta)
          }
        }

        compLong.elemUpdate(new L1Updater, new L1UpdateParam(theta, L, weight))
      }
    }
  }

  def pdgGetG(wVal: Double, gVal: Double, L: Double, theta: Double): Double = {
    val zVal = wVal - L * gVal

    val proxVal = if (zVal > theta) {
      zVal - theta
    } else if (zVal < -1 * theta) {
      zVal + theta
    } else {
      0
    }
    (wVal - proxVal) / L
  }
}
