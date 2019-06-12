package com.tencent.angel.ml.core.local.optimizer

import java.util.concurrent.Future

import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.local.variables.{LocalBlasMatVariable, LocalMatVariable, LocalVecVariable}
import com.tencent.angel.ml.core.variable.Variable
import com.tencent.angel.ml.core.optimizer.{Optimizer, OptimizerHelper}
import com.tencent.angel.ml.core.utils.{OptUtils, OptimizerKeys}
import com.tencent.angel.ml.math2.ufuncs.{OptFuncs, Ufuncs}
import org.json4s.JsonAST.{JObject, JValue}
import org.json4s.JsonDSL._
import com.tencent.angel.ml.core.utils.JsonUtils.{extract, fieldEqualClassName}

class Adam(override var lr: Double, beta: Double, gamma: Double) extends Optimizer {
  override val numSlot: Int = 3

  override def update[T](variable: Variable, epoch: Int, batchSize: Int): Future[T] = {
    val powBeta = Math.pow(beta, epoch + 1)
    val powGamma = Math.pow(gamma, epoch + 1)

    variable match {
      case v: LocalBlasMatVariable =>
        val value = v.storage.getRow(0)
        val mt = v.storage.getRow(1)
        val vt = v.storage.getRow(2)
        val grad = v.storage.getRow(3)

        OptFuncs.iexpsmoothing(mt, grad, beta)
        OptFuncs.iexpsmoothing2(vt, grad, gamma)
        value.isub(OptFuncs.adamdelta(mt, vt, powBeta, powGamma).imul(lr))
//        if (regL1Param != 0.0) {
//          Ufuncs.isoftthreshold(value, regL1Param)
//        }

        grad.imul(0.0)
      case v: LocalMatVariable =>
        val numFactors = v.numRows
        val value = OptUtils.getRowsAsMatrix(v.storage, 0, numFactors)
        val mt = OptUtils.getRowsAsMatrix(v.storage, numFactors, numFactors * 2)
        val vt = OptUtils.getRowsAsMatrix(v.storage, numFactors * 2, numFactors * 3)
        val grad = OptUtils.getRowsAsMatrix(v.storage, numFactors * 3, numFactors * 4)

        OptFuncs.iexpsmoothing(mt, grad, beta)
        OptFuncs.iexpsmoothing2(vt, grad, gamma)
        value.isub(OptFuncs.adamdelta(mt, vt, powBeta, powGamma).imul(lr))
//        if (regL1Param != 0.0) {
//          Ufuncs.isoftthreshold(value, regL1Param)
//        }

        grad.imul(0.0)
      case v: LocalVecVariable =>
        val value = v.storage.getRow(0)
        val mt = v.storage.getRow(1)
        val vt = v.storage.getRow(2)
        val grad = v.storage.getRow(3)

        OptFuncs.iexpsmoothing(mt, grad, beta)
        OptFuncs.iexpsmoothing2(vt, grad, gamma)
        value.isub(OptFuncs.adamdelta(mt, vt, powBeta, powGamma).imul(lr))

//        if (regL1Param != 0.0) {
//          Ufuncs.isoftthreshold(value, regL1Param)
//        }

        grad.imul(0.0)
    }
    null.asInstanceOf[Future[T]]
  }

  override def toJson: JObject = {
    (OptimizerKeys.typeKey -> s"${this.getClass.getSimpleName}") ~
      (OptimizerKeys.betaKey-> beta) ~
      (OptimizerKeys.gammaKey -> gamma)
  }
}

object Adam {
  private val conf: SharedConf = SharedConf.get()

  def fromJson(jast: JObject): Adam = {
    assert(fieldEqualClassName[Adam](jast, OptimizerKeys.typeKey))
    val beta = conf.getDouble(MLCoreConf.ML_OPT_ADAM_BETA, MLCoreConf.DEFAULT_ML_OPT_ADAM_BETA)
    val gamma = conf.getDouble(MLCoreConf.ML_OPT_ADAM_GAMMA, MLCoreConf.DEFAULT_ML_OPT_ADAM_GAMMA)

    new Adam(1.0, extract[Double](jast, OptimizerKeys.betaKey, Some(beta)).get,
      extract[Double](jast, OptimizerKeys.gammaKey, Some(gamma)).get
    )
  }
}
