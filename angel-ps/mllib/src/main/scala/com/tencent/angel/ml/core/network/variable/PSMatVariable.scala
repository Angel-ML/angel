package com.tencent.angel.ml.core.network.variable

import com.tencent.angel.ml.core.network.graph.Graph
import com.tencent.angel.ml.core.network.variable.Variable.Location
import com.tencent.angel.ml.core.optimizer.{OptUtils, Optimizer}
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.matrix.psf.update.RandomNormal
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.psagent.PSAgentContext
import java.util.concurrent.Future


class PSMatVariable(name: String, val numRows: Int, val numCols: Long, val validIndexNum: Long, val numSlot: Int,
                    rowType: RowType)(implicit graph: Graph)
  extends PSVariable(name, rowType)(graph) with MatVariable {
  protected var matrixId: Int = -1
  override protected var matrix: Matrix = _

  protected val numRowsInternal: Int = numRows * (numSlot + 1)
  protected val numColsInternal: Long = numCols
  override protected val rowsSaved: Array[Int] = if (numSlot > 0) (0 to numSlot).toArray else Array(0)
  override protected val ctx: MatrixContext = PSMatrixUtils.createPSMatrixCtx(name, numRowsInternal, numColsInternal, rowType)
  if (validIndexNum > 0) {
    ctx.setValidIndexNum(validIndexNum)
  }

  protected var mean: Double = 0.0
  protected var stddev: Double = 0.000001


  override def init(taskFlag: Int, mean: Double, stddev: Double): Unit = {
    this.mean = mean
    this.stddev = stddev

    if (matrixId == -1) {
      matrixId = PSMatrixUtils.getMatrixId(name)
    }

    if (taskFlag == 0) {
      storageType match {
        case "dense" | "component_dense" =>
          val randFunc = new RandomNormal(matrixId, 0, numRows, mean, stddev)
          PSAgentContext.get().getUserRequestAdapter.update(randFunc).get()
        case "sparse" | "component_sparse" =>
      }
    }
  }

  override def pullParams(epoch: Int, indices: Vector = null): Unit = {
    if (matrixId == -1) {
      matrixId = PSMatrixUtils.getMatrixId(name)
    }

    if (indices != null) {
      matrix = PSMatrixUtils.getMatrixWithIndex(epoch, matrixId, 0, numRows, indices)(mean, stddev)
    } else {
      matrix = PSMatrixUtils.getMatrix(epoch, matrixId, 0, numRows)
    }
  }

  override def pushGrads(features: Matrix, backward: Matrix): Unit = {
    val vectors = (0 until numRows).toArray.map { colId =>
      val weightRowGrad = backward match {
        case bw: BlasDoubleMatrix =>
          features.transDot(bw.getCol(colId)).imul(normal)
        case bw: BlasFloatMatrix =>
          features.transDot(bw.getCol(colId)).imul(normal)
      }

      weightRowGrad.setMatrixId(matrixId)
      weightRowGrad.setRowId(numRows * numSlot + colId)
      weightRowGrad.setClock(matrix.getClock)

      weightRowGrad
    }

    PSMatrixUtils.incrementRows(matrixId, vectors.map(_.getRowId), vectors)
  }

  override def update(optimizer: Optimizer, epoch: Int, batchSize: Int): Future[VoidResult] = {
    optimizer.update(matrixId, numRows, epoch, batchSize)
  }
}
