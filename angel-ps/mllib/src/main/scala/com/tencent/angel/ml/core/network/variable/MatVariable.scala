package com.tencent.angel.ml.core.network.variable

import java.util.concurrent.Future

import com.tencent.angel.ml.core.network.graph.Graph
import com.tencent.angel.ml.core.network.variable.Variable.Location
import com.tencent.angel.ml.core.network.variable.Variable.Location.Location
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.NotInitialException
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.model.{ModelLoadContext, ModelSaveContext}

import scala.language.implicitConversions

trait MatVariable {
  protected var matrix: Matrix

  def snapshot(): Matrix = {
    if (matrix == null) {
      throw NotInitialException("the matrix is not initialized!")
    }

    matrix
  }

  def init(taskFlag: Int, mean: Double, stddev: Double): Unit

  def pullParams(epoch: Int, indices: vector.Vector = null): Unit

  def pushGrads(features: Matrix, backward: Matrix): Unit

  def update(optimizer: Optimizer, epoch: Int, batchSize: Int): Future[VoidResult]

  def loadParams(loadContext: ModelLoadContext): Unit

  def saveParams(saveContext: ModelSaveContext): Unit
}

object MatVariable {
  def apply(name: String, numRows: Int, numCols: Long, validIndexNum: Long, numSlot: Int, rowType: RowType,
            matType: MatrixType.MatrixType, location: Location)(implicit graph: Graph): MatVariable = {
    (location, matType) match {
      case (Location.PS, MatrixType.Common) =>
        new PSMatVariable(name, numRows, numCols, validIndexNum, numSlot, rowType)
      case (Location.PS, MatrixType.Blas) =>
        new PSBlasMatVariable(name, numRows, numCols, numSlot, rowType)
      case (Location.PS, MatrixType.Embedding) =>
        new PSEmbedVariable(name, numRows, numCols, validIndexNum, numSlot, rowType)
      case (Location.Local, MatrixType.Common) =>
        new LocalMatVariable(name, numRows, numCols, numSlot, rowType)
      case (Location.Local, MatrixType.Blas) =>
        new LocalBlasMatVariable(name, numRows, numCols, numSlot, rowType)
      case (Location.Local, MatrixType.Embedding) =>
        new LocalEmbedVariable(name, numRows, numCols, numSlot, rowType)
    }
  }

  object MatrixType extends Enumeration {
    type MatrixType = Value
    val Common, Blas, Embedding = Value
  }

  implicit def toMatrix(v: MatVariable): Matrix = v.snapshot()
}
