package com.tencent.angel.ml.core.network.variable

import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.NotInitialException
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector
import com.tencent.angel.ml.math2.vector.Vector
import java.lang.{Long => JLong}
import java.util.concurrent.Future
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.{HashMap => JHashMap, Map => JMap}

import com.tencent.angel.ml.core.network.variable.MatrixType.Value
import com.tencent.angel.ml.core.network.variable.VarState.VarState
import com.tencent.angel.ml.core.network.{EvnContext, Graph}

import scala.language.implicitConversions


object MatrixType extends Enumeration {
  type MatrixType = Value
  val Common, Blas, Embedding = Value
}

object VarState extends Enumeration {
  type VarState = Value
  val New, Created, Initialized, Ready, Expired = Value
}

trait VoidType

trait Updater extends Serializable {
  val numSlot: Int

  def update[T](variable: Variable, epoch: Int, batchSize: Int): Future[T]
}

trait TrainCycle {
  protected var state: VarState = VarState.New
  protected val lock = new ReentrantReadWriteLock()
  protected val readLock: ReentrantReadWriteLock.ReadLock = lock.readLock()
  protected val writeLock: ReentrantReadWriteLock.WriteLock = lock.writeLock()

  def create(envCtx: EvnContext = null): Unit

  def load(envCtx: EvnContext, path: String): Unit

  def init(taskFlag: Int, mean: Double, stddev: Double): Unit

  def pull(epoch: Int, indices: vector.Vector = null): Unit

  def push(grad: Matrix, alpha: Double): Unit

  def update[T](epoch: Int, batchSize: Int): Future[T]

  def save(envCtx: EvnContext, path: String): Unit

  protected def transSate(from: VarState, to: VarState): Unit = {
    assert(state == from)
    state = to
  }
}

trait MatTrainCycle extends TrainCycle {
  protected var matrix: Matrix

  def snapshot(): Matrix = {
    assert(state == VarState.Ready)

    readLock.lock()
    try {
      if (matrix == null) {
        throw NotInitialException("the matrix is not initialized!")
      }

      matrix
    } finally {
      readLock.unlock()
    }
  }
}

trait VecTrainCycle extends TrainCycle {
  protected var vector: Vector

  def snapshot(): Vector = {
    assert(state == VarState.Ready)

    readLock.lock()
    try {
      if (vector == null) {
        throw NotInitialException("the vector is not initialized!")
      }

      vector
    } finally {
      readLock.unlock()
    }

  }
}


trait MatVariable extends MatTrainCycle

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


trait VecVariable extends VecTrainCycle

object VecVariable {
  implicit def toVector(v: VecVariable): Vector = v.snapshot()
}


abstract class Variable(val name: String, val rowType: RowType, val updater: Updater, val withInput: Boolean)(implicit val graph: Graph)
  extends TrainCycle {
  graph.addVariable(this)

  protected val numSlot: Int = if (updater == null) {
    0
  } else {
    updater.numSlot
  }
}
