package com.tencent.angel.ml.core.local.optimizer

import java.util.concurrent.Future

import com.tencent.angel.ml.core.local.OptimizerKeys
import com.tencent.angel.ml.core.local.variables.{LocalBlasMatVariable, LocalMatVariable, LocalVecVariable}
import com.tencent.angel.ml.core.network.variable.Variable
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.JsonUtils.{extract, fieldEqualClassName}
import com.tencent.angel.ml.core.utils.OptUtils
import com.tencent.angel.ml.math2.ufuncs.{OptFuncs, Ufuncs}
import org.json4s.JsonAST.{JObject, JValue}
import org.json4s.JsonDSL._


class Momentum(override var lr: Double, val momentum: Double = 0.9) extends Optimizer {
  override val numSlot: Int = 2
  override protected var regL1Param: Double = 0.0
  override protected var regL2Param: Double = 0.0

  override def update[T](variable: Variable, epoch: Int): Future[T] = {
    variable match {
      case v: LocalBlasMatVariable =>
        val value = v.storage.getRow(0)
        val moment = v.storage.getRow(1)
        val grad = v.storage.getRow(2)
//        val grad = if (regL2Param == 0) {
//          v.storage.getRow(2)
//        } else {
//          value.axpy(v.storage.getRow(2), regL2Param)
//        }

        OptFuncs.iexpsmoothing(moment, grad, momentum)
        value.isub(moment.mul(lr))
//        if (regL1Param != 0.0) {
//          Ufuncs.isoftthreshold(value, regL1Param)
//        }

        grad.imul(0.0)
      case v: LocalMatVariable =>
        val numFactors: Int = v.numRows
        val value = OptUtils.getRowsAsMatrix(v.storage, 0, numFactors)
        val moment = OptUtils.getRowsAsMatrix(v.storage, numFactors, numFactors * 2)
        val grad = OptUtils.getRowsAsMatrix(v.storage, numFactors * 2, numFactors * 3)
//        val grad = if (regL2Param == 0) {
//          OptUtils.getRowsAsMatrix(v.storage, numFactors, numFactors * 3)
//        } else {
//          value.axpy(OptUtils.getRowsAsMatrix(v.storage, numFactors, numFactors * 3), regL2Param)
//        }

        OptFuncs.iexpsmoothing(moment, grad, momentum)
        value.isub(moment.mul(lr))
//        if (regL1Param != 0.0) {
//          Ufuncs.isoftthreshold(value, regL1Param)
//        }

        grad.imul(0.0)
      case v: LocalVecVariable =>
        val value = v.storage.getRow(0)
        val moment = v.storage.getRow(1)
        val grad = v.storage.getRow(2)
//        val grad = if (regL2Param == 0) {
//          v.storage.getRow(2)
//        } else {
//          value.axpy(v.storage.getRow(2), regL2Param)
//        }

        OptFuncs.iexpsmoothing(moment, grad, momentum)
        value.isub(moment.mul(lr))
//        if (regL1Param != 0.0) {
//          Ufuncs.isoftthreshold(value, regL1Param)
//        }

        grad.imul(0.0)
    }

    null.asInstanceOf[Future[T]]
  }

  override def toJson: JObject = {
    ("type" -> s"${this.getClass.getSimpleName}") ~
      ("momentum" -> momentum)
  }
}

object Momentum {
  def fromJson(jast: JObject): Momentum = {
    assert(fieldEqualClassName[Momentum](jast, OptimizerKeys.typeKey))
    new Momentum(0.0001, extract[Double](jast, "momentum", Some(0.9)).get)
  }
}
