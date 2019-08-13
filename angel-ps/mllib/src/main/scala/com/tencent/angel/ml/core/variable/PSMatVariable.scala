package com.tencent.angel.ml.core.variable

import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.ml.matrix.psf.update.RandomNormal
import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.mlcore.variable._
import com.tencent.angel.psagent.PSAgentContext


class PSMatVariable(name: String,
                    val numRows: Int,
                    val numCols: Long,
                    val validIndexNum: Long,
                    updater: Updater,
                    rowType: RowType,
                    formatClassName: String,
                    allowPullWithIndex: Boolean)
                   (implicit  conf: SharedConf, variableManager: VariableManager, cilsImpl: CILSImpl)
  extends PSVariable(name, rowType, updater, formatClassName, allowPullWithIndex) with MatVariable {

  override val numFactors: Int = numRows
  override protected var matrix: Matrix = _

  protected override def rowsSaved(withSlot: Boolean): Array[Int] = {
    if (withSlot && numSlot > 0) {
      (0 until numRows * numSlot).toArray
    } else {
      (0 until numRows).toArray
    }
  }

  def getMatrixCtx: MatrixContext = {
    if (ctx == null) {
      val numRowsInternal = numRows * (numSlot + 1)
      val numColsInternal = numCols

      PSMatrixUtils.createPSMatrixCtx(name, numRowsInternal, numColsInternal,
        rowType, formatClassName, validIndexNum)
    } else {
      ctx
    }
  }

  protected override def doInit(taskFlag: Int): Unit = {
    if (taskFlag == 0 && rowType.isDense) {
      val randFunc = new RandomNormal(matrixId, 0, numRows, mean, stddev)
      PSAgentContext.get().getUserRequestAdapter.update(randFunc).get()
    }
  }

  protected override def doPull(epoch: Int, indices: Vector): Unit = {
    if (indices != null) {
      matrix = PSMatrixUtils.getMatrixWithIndex(epoch, matrixId, 0, numRows, indices)(mean, stddev)
    } else {
      matrix = PSMatrixUtils.getMatrix(epoch, matrixId, 0, numRows)
    }
  }

  protected override def doPush(grad: Matrix, alpha: Double): Unit = {
    val rowIds = (0 until grad.getNumRows).toArray.map { idx => idx + numSlot * numRows }
    val vectors = (0 until grad.getNumRows).toArray.map { idx =>
      if (numSlot == 0) {
        val vector = grad.getRow(idx).imul(-alpha)
        vector.setMatrixId(matrixId)
        vector.setRowId(idx + numSlot * numRows)
        vector
      } else {
        val vector = grad.getRow(idx)
        vector.setMatrixId(matrixId)
        vector.setRowId(idx + numSlot * numRows)
        vector
      }
    }

    PSMatrixUtils.incrementRows(matrixId, rowIds, vectors)
  }


}
