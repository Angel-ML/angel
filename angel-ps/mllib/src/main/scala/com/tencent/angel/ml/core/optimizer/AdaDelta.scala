package com.tencent.angel.ml.core.optimizer

import java.util.concurrent.Future

import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.ml.psf.optimizer.AdaDeltaUpdateFunc
import com.tencent.angel.psagent.PSAgentContext
import org.apache.commons.logging.LogFactory

class AdaDelta(stepSize: Double, val beta: Double = 0.9) extends Optimizer(stepSize) {
  private val LOG = LogFactory.getLog(classOf[AdaDelta])
  override protected var numSlot: Int = 3

  override def update(matrixId: Int, numFactors: Int, epoch: Int): Future[VoidResult] = {

    val func = new AdaDeltaUpdateFunc(matrixId, numFactors, epsilon, beta, lr, regL1Param, regL2Param, epoch)
    PSAgentContext.get().getUserRequestAdapter.update(func)
  }

  override def update(matrixId: Int, numFactors: Int, epoch: Int, batchSize: Int): Future[VoidResult] = {
    val func = new AdaDeltaUpdateFunc(matrixId, numFactors, epsilon, beta, lr, regL1Param, regL2Param, epoch, batchSize)
    PSAgentContext.get().getUserRequestAdapter.update(func)
  }

  override def toString: String = {
    s"AdaDelta beta=$beta lr=$lr regL2=$regL2Param regL1=$regL1Param epsilon=$epsilon"
  }
}
