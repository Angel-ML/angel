package com.tencent.angel.ml.core.local.optimizer

import java.util.concurrent.Future

import com.tencent.angel.ml.core.local.variables.{LocalBlasMatVariable, LocalMatVariable, LocalVecVariable}
import com.tencent.angel.ml.core.network.variable.{Variable, VecVariable}
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.OptUtils
import com.tencent.angel.ml.math2.ufuncs.{OptFuncs, Ufuncs}

class SGD(override var lr: Double) extends Optimizer{
  override val numSlot: Int = 1
  override protected var regL1Param: Double = 0.0
  override protected var regL2Param: Double = 0.0

  override def update[T](variable: Variable, numFactors: Int, epoch: Int): Future[T] = {
    variable match {
      case v: LocalBlasMatVariable =>
        val value = v.storage.getRow(0)
        val grad = if (regL2Param == 0) {
          v.storage.getRow(1)
        } else {
          value.axpy(v.storage.getRow(1), regL2Param)
        }

        value.isub(grad.imul(lr))
        if (regL1Param != 0.0) {
          Ufuncs.isoftthreshold(value, regL1Param)
        }

        grad.clear()
      case v: LocalMatVariable =>
        val value = OptUtils.getRowsAsMatrix(v.storage, 0, numFactors)
        val grad = if (regL2Param == 0) {
          OptUtils.getRowsAsMatrix(v.storage, numFactors, numFactors * 2)
        } else {
          value.axpy(OptUtils.getRowsAsMatrix(v.storage, numFactors, numFactors * 2), regL2Param)
        }

        value.isub(grad.imul(lr))
        if (regL1Param != 0.0) {
          Ufuncs.isoftthreshold(value, regL1Param)
        }

        grad.clear()

      case v: LocalVecVariable =>
        val value = v.storage.getRow(0)
        val grad = if (regL2Param == 0) {
          v.storage.getRow(1)
        } else {
          value.axpy(v.storage.getRow(1), regL2Param)
        }

        value.isub(grad.imul(lr))
        if (regL1Param != 0.0) {
          Ufuncs.isoftthreshold(value, regL1Param)
        }

        grad.clear()
    }

    null.asInstanceOf[Future[T]]
  }
}
