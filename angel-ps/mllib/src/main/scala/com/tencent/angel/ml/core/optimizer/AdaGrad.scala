package com.tencent.angel.ml.core.optimizer

import java.util.concurrent.Future

import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.ml.psf.optimizer.AdaGradUpdateFunc
import com.tencent.angel.psagent.PSAgentContext
import org.apache.commons.logging.LogFactory

class AdaGrad(stepSize: Double, val beta: Double = 0.9) extends Optimizer(stepSize) {
  private val LOG = LogFactory.getLog(classOf[AdaGrad])
  override protected var numSlot: Int = 2

  override def update(matrixId: Int, numFactors: Int, epoch: Int): Future[VoidResult] = {

    val func = new AdaGradUpdateFunc(matrixId, numFactors, epsilon, beta, lr, regL1Param, regL2Param, epoch)
    PSAgentContext.get().getUserRequestAdapter.update(func)
  }

  override def update(matrixId: Int, numFactors: Int, epoch: Int, batchSize: Int): Future[VoidResult] = {
    val func = new AdaGradUpdateFunc(matrixId, numFactors, epsilon, beta, lr, regL1Param, regL2Param, epoch, batchSize)
    PSAgentContext.get().getUserRequestAdapter.update(func)
  }

  override def toString: String = {
    s"AdaGrad beta=$beta lr=$lr regL2=$regL2Param regL1=$regL1Param epsilon=$epsilon"
  }
}
