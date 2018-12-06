package com.tencent.angel.ml.core.local.optimizer

import java.util.concurrent.Future

import com.tencent.angel.ml.core.local.variables.{LocalBlasMatVariable, LocalMatVariable, LocalVecVariable}
import com.tencent.angel.ml.core.network.variable.Variable
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.OptUtils
import com.tencent.angel.ml.math2.ufuncs.{OptFuncs, Ufuncs}

class Adam(override var lr: Double, beta: Double, gamma: Double) extends Optimizer {
  override val numSlot: Int = 3
  override protected var regL1Param: Double = _
  override protected var regL2Param: Double = _

  override def update[T](variable: Variable, numFactors: Int, epoch: Int): Future[T] = {
    val powBeta = Math.pow(beta, epoch + 1)
    val powGamma = Math.pow(gamma, epoch + 1)

    variable match {
      case v: LocalBlasMatVariable =>
        val value = v.storage.getRow(0)
        val mt = v.storage.getRow(1)
        val vt = v.storage.getRow(2)
        val grad = if (regL2Param == 0) {
          v.storage.getRow(3)
        } else {
          value.axpy(v.storage.getRow(3), regL2Param)
        }

        OptFuncs.iexpsmoothing(mt, grad, beta)
        OptFuncs.iexpsmoothing2(mt, grad, gamma)
        value.isub(OptFuncs.adamdelta(mt, vt, powBeta, powGamma).imul(lr))
        if (regL1Param != 0.0) {
          Ufuncs.isoftthreshold(value, regL1Param)
        }

        grad.clear()
      case v: LocalMatVariable =>
        val value = OptUtils.getRowsAsMatrix(v.storage, 0, numFactors)
        val mt = OptUtils.getRowsAsMatrix(v.storage, numFactors, numFactors * 2)
        val vt = OptUtils.getRowsAsMatrix(v.storage, numFactors * 2, numFactors * 3)
        val grad = if (regL2Param == 0) {
          OptUtils.getRowsAsMatrix(v.storage, numFactors * 3, numFactors * 4)
        } else {
          value.axpy(OptUtils.getRowsAsMatrix(v.storage, numFactors * 3, numFactors * 4), regL2Param)
        }

        OptFuncs.iexpsmoothing(mt, grad, beta)
        OptFuncs.iexpsmoothing2(mt, grad, gamma)
        value.isub(OptFuncs.adamdelta(mt, vt, powBeta, powGamma).imul(lr))
        if (regL1Param != 0.0) {
          Ufuncs.isoftthreshold(value, regL1Param)
        }

        grad.clear()
      case v: LocalVecVariable =>
        val value = v.storage.getRow(0)
        val mt = v.storage.getRow(1)
        val vt = v.storage.getRow(2)
        val grad = if (regL2Param == 0) {
          v.storage.getRow(3)
        } else {
          value.axpy(v.storage.getRow(3), regL2Param)
        }

        OptFuncs.iexpsmoothing(mt, grad, beta)
        OptFuncs.iexpsmoothing2(mt, grad, gamma)
        value.isub(OptFuncs.adamdelta(mt, vt, powBeta, powGamma).imul(lr))

        if (regL1Param != 0.0) {
          Ufuncs.isoftthreshold(value, regL1Param)
        }

        grad.clear()
    }
    null.asInstanceOf[Future[T]]
  }
}
