package com.tencent.angel.ml.core.network.variable

import java.util.concurrent.Future

import com.tencent.angel.ml.core.network.graph.Graph
import com.tencent.angel.ml.core.network.variable.Variable.Location
import com.tencent.angel.ml.core.network.variable.Variable.Location.Location
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.NotInitialException
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.model.{ModelLoadContext, ModelSaveContext}


trait VecVariable {
  protected var vector: Vector

  def snapshot(): Vector = {
    if (vector == null) {
      throw NotInitialException("the vector is not initialized!")
    }

    vector
  }

  def init(taskFlag: Int, mean: Double, stddev: Double): Unit

  def pullParams(epoch: Int, indices: Vector = null): Unit

  def pushGrads(backward: Matrix, lr: Double): Unit

  def pushGrads(grad: Vector, lr: Double): Unit

  def update(optimizer: Optimizer, epoch: Int, batchSize: Int): Future[VoidResult]

  def loadParams(loadContext: ModelLoadContext): Unit

  def saveParams(saveContext: ModelSaveContext): Unit
}

object VecVariable {
  def apply(name: String, length: Long, numSlot: Int, rowType: RowType, location: Location)(implicit graph: Graph): VecVariable = {
    location match {
      case Location.PS => new PSVecVariable(name, length, numSlot, rowType)(graph)
      case Location.Local => new LocalVecVariable(name, length, numSlot, rowType)(graph)
    }
  }


  implicit def toVector(v: VecVariable): Vector = v.snapshot()
}