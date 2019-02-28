package com.tencent.angel.ml.core.variable

import com.tencent.angel.matrix.MatrixContext
import com.tencent.angel.matrix.psf.update.RandomNormal
import com.tencent.angel.ml.core.utils.{PSMatrixUtils, RowTypeUtils}
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.psagent.PSAgentContext


class PSBlasMatVariable(name: String,
                        val numRows: Int,
                        val numCols: Long,
                        updater: Updater,
                        rowType: RowType,
                        formatClassName: String,
                        allowPullWithIndex: Boolean)
                       (implicit variableManager: VariableManager)
  extends PSVariable(name, rowType, updater, formatClassName, allowPullWithIndex) with BlasMatVariable {
  override val numFactors: Int = 1
  override protected var matrix: Matrix = _

  protected override def rowsSaved(withSlot: Boolean): Array[Int] = {
    if (withSlot && numSlot > 0) {
      (0 until numSlot).toArray
    } else {
      Array(0)
    }
  }

  def getMatrixCtx: MatrixContext = {
    if (ctx == null) {
      val numRowsInternal = numSlot + 1
      val numColsInternal = numRows * numCols

      PSMatrixUtils.createPSMatrixCtx(name, numRowsInternal, numColsInternal,
        RowTypeUtils.getDenseModelType(rowType))
    } else {
      ctx
    }
  }

  protected override def doInit(taskFlag: Int): Unit = {
    if (taskFlag == 0 && rowType.isDense) {
      val randFunc = new RandomNormal(matrixId, 0, 1, mean, stddev)
      PSAgentContext.get().getUserRequestAdapter.update(randFunc).get()
    }
  }

  protected override def doPull(epoch: Int, indices: Vector): Unit = {
    matrix = PSMatrixUtils.getRowAsMatrix(epoch, matrixId, 0, numRows, numCols.toInt)
  }

  protected override def doPush(grad: Matrix, alpha: Double): Unit = {
    if (numSlot == 0) {
      PSMatrixUtils.incrementRowByMatrix(matrixId, 0, grad.imul(-alpha))
    } else {
      PSMatrixUtils.incrementRowByMatrix(matrixId, numSlot, grad)
    }
  }
}
