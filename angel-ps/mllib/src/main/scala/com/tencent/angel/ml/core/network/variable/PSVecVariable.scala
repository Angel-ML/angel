package com.tencent.angel.ml.core.network.variable


import java.util.concurrent.Future

import com.tencent.angel.ml.core.network.graph.Graph
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ml.matrix.psf.update.RandomNormal
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.psagent.PSAgentContext

class PSVecVariable(name: String, length: Long, numSlot: Int, rowType: RowType)(implicit graph: Graph)
  extends PSVariable(name, rowType)(graph) with VecVariable {
  protected var vectorId: Int = -1
  override protected var vector: Vector = _

  override protected val rowsSaved: Array[Int] = Array(0)
  override protected val ctx: MatrixContext = PSMatrixUtils.createPSMatrixCtx(name, numSlot + 1, length, rowType)

  protected var mean: Double = 0.0
  protected var stddev: Double = 0.000001

  override def init(taskFlag: Int, mean: Double, stddev: Double): Unit = {
    this.mean = mean
    this.stddev = stddev

    if (vectorId == -1) {
      vectorId = PSMatrixUtils.getMatrixId(name)
    }

    if (taskFlag == 0) {
      storageType match {
        case "dense" | "component_dense" =>
          val randFunc = new RandomNormal(vectorId, 0, 1, mean, stddev)
          PSAgentContext.get().getUserRequestAdapter.update(randFunc).get()
        case "sparse" | "component_sparse" =>
      }
    }
  }

  def pullParams(epoch: Int, indices: Vector = null): Unit = {
    if (vectorId == -1) {
      vectorId = PSMatrixUtils.getMatrixId(name)
    }

    if (indices != null) {
      vector = PSMatrixUtils.getRowWithIndex(epoch, vectorId, 0, indices)(mean, stddev)
    } else {
      vector = PSMatrixUtils.getRow(epoch, vectorId, 0)
    }
  }

  def pushGrads(backward: Matrix, lr: Double): Unit = {
    if (numSlot <= 0) {
      PSMatrixUtils.incrementRow(vectorId, 0, backward.sum(0).imul(-lr * normal))
    } else {
      PSMatrixUtils.incrementRow(vectorId, numSlot, backward.sum(0).imul(normal))
    }
  }

  def pushGrads(grad: Vector, lr: Double): Unit = {
    if (numSlot <= 0) {
      PSMatrixUtils.incrementRow(vectorId, 0, grad.imul(-lr / graph.taskNum))
    } else {
      PSMatrixUtils.incrementRow(vectorId, numSlot, grad.imul(1.0 / graph.taskNum))
    }
  }

  override def update(optimizer: Optimizer, epoch: Int, batchSize: Int): Future[VoidResult] = {
    if (numSlot > 0) {
      optimizer.update(vectorId, 1, epoch, batchSize)
    } else {
      null.asInstanceOf[Future[VoidResult]]
    }
  }
}
