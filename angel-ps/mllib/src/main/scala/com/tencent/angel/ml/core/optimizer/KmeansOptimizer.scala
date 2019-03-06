package com.tencent.angel.ml.core.optimizer
import java.util.concurrent.Future


import com.tencent.angel.ml.core.utils.JsonUtils.fieldEqualClassName
import com.tencent.angel.ml.core.utils.OptimizerKeys
import com.tencent.angel.ml.core.variable.{PSVariable, Variable}
import com.tencent.angel.ml.psf.optimizer.{KmeansUpdateFunc}
import com.tencent.angel.psagent.PSAgentContext
import org.apache.commons.logging.LogFactory
import org.json4s.JsonAST.{JField, JObject, JString}

class KmeansOptimizer() extends Optimizer {
  private val LOG = LogFactory.getLog(classOf[KmeansOptimizer])
  override var lr: Double = _
  override val numSlot: Int = 1

  override def update[T](variable: Variable, epoch: Int, batchSize: Int): Future[T] = {
    val matrixId = variable.asInstanceOf[PSVariable].getMatrixId
    val func = new KmeansUpdateFunc(matrixId, variable.asInstanceOf[PSVariable].numFactors, batchSize)
    PSAgentContext.get().getUserRequestAdapter.update(func).asInstanceOf[Future[T]]
  }

  override def toString: String = {
    s"KmeansOptimizer"
  }

  override def toJson: JObject = {
    JObject(JField(OptimizerKeys.typeKey, JString(s"${this.getClass.getSimpleName}")))
  }
}


object KmeansOptimizer {

  def fromJson(jast: JObject): KmeansOptimizer = {
    assert(fieldEqualClassName[KmeansOptimizer](jast, OptimizerKeys.typeKey))

    new KmeansOptimizer()
  }
}