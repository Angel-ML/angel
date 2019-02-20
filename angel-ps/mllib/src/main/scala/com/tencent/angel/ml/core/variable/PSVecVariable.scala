package com.tencent.angel.ml.core.variable


import com.tencent.angel.matrix.MatrixContext
import com.tencent.angel.matrix.psf.update.RandomNormal
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.psagent.PSAgentContext

class PSVecVariable(name: String, length: Long, val validIndexNum: Long, updater: Updater, rowType: RowType,
                    formatClassName: String, allowPullWithIndex: Boolean)(implicit graph: Graph)
  extends PSVariable(name, rowType, updater, formatClassName, allowPullWithIndex)(graph)
    with VecVariable {
  override val numFactors: Int = 1
  override protected var vector: Vector = _

  def getMatrixCtx: MatrixContext = {
    if (ctx == null) {
      val ctx_ = PSMatrixUtils.createPSMatrixCtx(name, numSlot + 1, length, rowType)
      ctx_.setValidIndexNum(validIndexNum)
      ctx_
    } else {
      ctx
    }
  }

  protected override def rowsSaved(withSlot: Boolean): Array[Int] = {
    if (withSlot && numSlot > 0) {
      (0 until numSlot).toArray
    } else {
      Array(0)
    }
  }

  protected override def doInit(taskFlag: Int): Unit = {
    if (taskFlag == 0 && rowType.isDense) {
      val randFunc = new RandomNormal(matrixId, 0, 1, mean, stddev)
      PSAgentContext.get().getUserRequestAdapter.update(randFunc).get()
    }
  }

  protected override def doPull(epoch: Int, indices: Vector = null): Unit = {
    if (indices != null) {
      vector = PSMatrixUtils.getRowWithIndex(epoch, matrixId, 0, indices)(mean, stddev)
    } else {
      vector = PSMatrixUtils.getRow(epoch, matrixId, 0)
    }
  }

  protected override def doPush(grad: Matrix, alpha: Double): Unit = {
    if (numSlot <= 0) {
      val vector = grad.getRow(0).imul(-alpha)
      vector.setMatrixId(matrixId)
      vector.setRowId(0)
      PSMatrixUtils.incrementRow(matrixId, 0, vector)
    } else {
      val vector = grad.getRow(0)
      vector.setMatrixId(matrixId)
      vector.setRowId(numSlot)
      PSMatrixUtils.incrementRow(matrixId, numSlot, vector)
    }
  }


}
