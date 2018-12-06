package com.tencent.angel.ml.core.local.optimizer

import java.util.concurrent.Future

import com.tencent.angel.ml.core.local.variables.{LocalBlasMatVariable, LocalMatVariable, LocalVecVariable}
import com.tencent.angel.ml.core.network.variable.Variable
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.OptUtils
import com.tencent.angel.ml.math2.ufuncs.{OptFuncs, Ufuncs}

class Momentum(override var lr: Double, val momentum: Double = 0.9) extends Optimizer {
  override val numSlot: Int = 2
  override protected var regL1Param: Double = 0.0
  override protected var regL2Param: Double = 0.0

  override def update[T](variable: Variable, numFactors: Int, epoch: Int): Future[T] = {
    variable match {
      case v: LocalBlasMatVariable =>
        val value = v.storage.getRow(0)
        val moment = v.storage.getRow(1)
        val grad = if (regL2Param == 0) {
          v.storage.getRow(1)
        } else {
          value.axpy(v.storage.getRow(1), regL2Param)
        }

        OptFuncs.iexpsmoothing(moment, grad, momentum)
        value.isub(moment.mul(lr))
        if (regL1Param != 0.0) {
          Ufuncs.isoftthreshold(value, regL1Param)
        }

        grad.clear()
      case v: LocalMatVariable =>
        val value = OptUtils.getRowsAsMatrix(v.storage, 0, numFactors)
        val moment = OptUtils.getRowsAsMatrix(v.storage, numFactors, numFactors * 2)
        val grad = if (regL2Param == 0) {
          OptUtils.getRowsAsMatrix(v.storage, numFactors, numFactors * 3)
        } else {
          value.axpy(OptUtils.getRowsAsMatrix(v.storage, numFactors, numFactors * 3), regL2Param)
        }

        OptFuncs.iexpsmoothing(moment, grad, momentum)
        value.isub(moment.mul(lr))
        if (regL1Param != 0.0) {
          Ufuncs.isoftthreshold(value, regL1Param)
        }

        grad.clear()
      case v: LocalVecVariable =>
        val value = v.storage.getRow(0)
        val moment = v.storage.getRow(1)
        val grad = if (regL2Param == 0) {
          v.storage.getRow(1)
        } else {
          value.axpy(v.storage.getRow(1), regL2Param)
        }

        OptFuncs.iexpsmoothing(moment, grad, momentum)
        value.isub(moment.mul(lr))
        if (regL1Param != 0.0) {
          Ufuncs.isoftthreshold(value, regL1Param)
        }
        grad.clear()
    }

    null.asInstanceOf[Future[T]]
  }
}
