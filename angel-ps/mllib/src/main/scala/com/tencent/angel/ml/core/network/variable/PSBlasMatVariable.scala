package com.tencent.angel.ml.core.network.variable

import java.util.concurrent.Future

import com.tencent.angel.ml.core.network.graph.Graph
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ml.matrix.psf.update.RandomNormal
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.psagent.PSAgentContext


class PSBlasMatVariable(name: String, val numRows: Int, val numCols: Long, val numSlot: Int, rowType: RowType)(
  implicit graph: Graph) extends PSVariable(name, rowType) with MatVariable {
  private var matrixId: Int = -1
  override protected var matrix: Matrix = _

  private val numRowsInternal: Int = numSlot + 1
  private val numColsInternal: Long = numRows * numCols
  override protected var rowsSaved: Array[Int] = Array(0)
  override protected var ctx: MatrixContext = PSMatrixUtils.createPSMatrixCtx(name, numRowsInternal, numColsInternal, rowType)

  private val normal = 1.0 / graph.getNormal

  override def init(taskFlag: Int, mean: Double, stddev: Double): Unit = {
    if (matrixId == -1) {
      matrixId = PSMatrixUtils.getMatrixId(name)
    }

    if (taskFlag == 0) {
      val randFunc = new RandomNormal(matrixId, 0, 1, mean, stddev)
      PSAgentContext.get().getUserRequestAdapter.update(randFunc).get()
    }
  }

  override def pullParams(epoch: Int, indices: Vector = null): Unit = {
    if (matrixId == -1) {
      matrixId = PSMatrixUtils.getMatrixId(name)
    }

    matrix = PSMatrixUtils.getRowAsMatrix(epoch, matrixId, 0, numRows, numCols.toInt)
  }

  override def pushGrads(features: Matrix, backward: Matrix): Unit = {
    val grad: Matrix = Ufuncs.dot(features, true, backward, false).imul(normal)
    PSMatrixUtils.incrementRowByMatrix(matrixId, numSlot, grad)
  }

  override def update(optimizer: Optimizer, epoch: Int, batchSize: Int): Future[VoidResult] = {
    optimizer.update(matrixId, 1, epoch, batchSize)
  }


}
