package com.tencent.angel.ml.core.network.variable

import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.NotInitialException
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector
import com.tencent.angel.ml.math2.vector.Vector
import java.lang.{Long => JLong}
import java.util.concurrent.Future
import java.util.{HashMap => JHashMap, Map => JMap}

import com.tencent.angel.ml.core.network.Graph

import scala.language.implicitConversions


object MatrixType extends Enumeration {
  type MatrixType = Value
  val Common, Blas, Embedding = Value
}

trait MatPushGrads {
  def pushGrads(features: Matrix, backward: Matrix): Unit

  def pushGrads(grad: Matrix, lr: Double): Unit
}

trait VecPushGrads {
  def pushGrads(backward: Matrix, lr: Double): Unit

  def pushGrads(grad: Vector, lr: Double): Unit
}

trait TrainCycle {
  def init(taskFlag: Int, mean: Double, stddev: Double): Unit

  def pullParams(epoch: Int, indices: vector.Vector = null): Unit

  def update[T](optimizer: Optimizer, epoch: Int, batchSize: Int): Future[T]
}


trait MatVariable extends TrainCycle with MatPushGrads {
  protected var matrix: Matrix

  def snapshot(): Matrix = {
    if (matrix == null) {
      throw NotInitialException("the matrix is not initialized!")
    }

    matrix
  }
}

object MatVariable {
  implicit def toMatrix(v: MatVariable): Matrix = v.snapshot()
}


trait EmbedVariable extends MatVariable {
  protected val embeddings: JMap[JLong, Vector] = new JHashMap[JLong, Vector]()
}

object EmbedVariable {
  implicit def toMatrix(v: EmbedVariable): Matrix = v.snapshot()
}


trait BlasMatVariable extends MatVariable

object BlasMatVariable {
  implicit def toMatrix(v: BlasMatVariable): Matrix = v.snapshot()
}


trait VecVariable extends TrainCycle with VecPushGrads {
  protected var vector: Vector

  def snapshot(): Vector = {
    if (vector == null) {
      throw NotInitialException("the vector is not initialized!")
    }

    vector
  }
}

object VecVariable {
  implicit def toVector(v: VecVariable): Vector = v.snapshot()
}


abstract class Variable(val name: String, val rowType: RowType)(implicit graph: Graph) {
  graph.addVariable(this)
}
