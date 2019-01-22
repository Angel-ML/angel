package com.tencent.angel.ml.core.local.variables

import java.util.Random
import java.util.concurrent.Future

import com.tencent.angel.ml.core.network.{EvnContext, Graph}
import com.tencent.angel.ml.core.network.variable.{BlasMatVariable, Updater, VarState}
import com.tencent.angel.ml.core.utils.{OptUtils, ValueNotAllowed}
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.storage.{IntDoubleDenseVectorStorage, IntFloatDenseVectorStorage}
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.{MFactory, StorageType, vector}


class LocalBlasMatVariable(name: String, val numRows: Int, val numCols: Long, updater: Updater,
                           rowType: RowType, withInput: Boolean)(implicit graph: Graph)
  extends LocalVariable(name, rowType, updater, withInput) with BlasMatVariable {
  override protected var matrix: Matrix = _

  override def create(envCtx: EvnContext): Unit = {
    writeLock.lock()

    try {
      if (state == VarState.New || state == VarState.Expired) {
        storage = graph.valueType match {
          case "float" =>
            MFactory.rbIntFloatMatrix(numSlot + 1, (numRows * numCols).toInt, StorageType.DENSE)
          case "double" =>
            MFactory.rbIntDoubleMatrix(numSlot + 1, (numRows * numCols).toInt, StorageType.DENSE)
          case _ => throw ValueNotAllowed("Value Not Allowed, Only Float/Double Are Allowed!")
        }

        // trans state
        if (state == VarState.New) {
          transSate(VarState.New, VarState.Created)
        } else {
          transSate(VarState.Expired, VarState.Created)
        }
      }

      assert(state == VarState.Created)
    } finally {
      writeLock.unlock()
    }

  }

  override def init(taskFlag: Int, mean: Double, stddev: Double): Unit = this.synchronized {
    writeLock.lock()

    try {
      if (state == VarState.Created) {
        if (taskFlag == 0) {
          val random = new Random()
          storage.getRow(0).getStorage match {
            case s: IntDoubleDenseVectorStorage =>
              val values = s.getValues
              values.indices.foreach { idx =>
                values(idx) = random.nextDouble() * stddev + mean
              }
            case s: IntFloatDenseVectorStorage =>
              val values = s.getValues
              values.indices.foreach { idx =>
                values(idx) = (random.nextDouble() * stddev + mean).toFloat
              }
            case _ =>
          }
        }

        // trans stats
        transSate(VarState.Created, VarState.Initialized)
      }

      assert(state == VarState.Initialized)
    } finally {
      writeLock.unlock()
    }
  }

  override def pull(epoch: Int, indices: vector.Vector = null): Unit = {
    writeLock.lock()

    try {
      if (state == VarState.Initialized || state == VarState.Ready) {
        assert(indices == null)
        if (matrix == null) {
          matrix = storage.getRow(0).getStorage match {
            case s: IntDoubleDenseVectorStorage =>
              MFactory.denseDoubleMatrix(numRows, numCols.toInt, s.getValues)
            case s: IntFloatDenseVectorStorage =>
              MFactory.denseFloatMatrix(numRows, numCols.toInt, s.getValues)
            case _ => throw ValueNotAllowed("Value Not Allowed, Only Float/Double Are Allowed!")
          }
        }

        // trans state
        if (state == VarState.Initialized) {
          transSate(VarState.Initialized, VarState.Ready)
        }
      }

      assert(state == VarState.Ready)
    } finally {
      writeLock.unlock()
    }

  }

  def push(features: Matrix, backward: Matrix): Unit = {
    writeLock.lock()

    try {
      assert(state == VarState.Ready)
      val grad: Matrix = Ufuncs.dot(backward, true, features, false).imul(graph.normalFactor)
      OptUtils.getRowAsMatrix(storage, numSlot, numRows, numCols.toInt).iadd(grad)
    } finally {
      writeLock.unlock()
    }

  }

  override def push(grad: Matrix, alpha: Double): Unit = {
    writeLock.lock()

    try {
      assert(state == VarState.Ready)
      if (numSlot == 0) {
        OptUtils.getRowAsMatrix(storage, numSlot, numRows, numCols.toInt).isub(grad.imul(alpha))
      } else {
        OptUtils.getRowAsMatrix(storage, numSlot, numRows, numCols.toInt).iadd(grad)
      }
    } finally {
      writeLock.unlock()
    }
  }

  override def update[T](epoch: Int, batchSize: Int): Future[T] = {
    writeLock.lock()

    try {
      assert(state == VarState.Ready)
      if (numSlot > 0) {
        updater.update[T](this, epoch, batchSize)
      } else {
        null.asInstanceOf[Future[T]]
      }
    } finally {
      writeLock.unlock()
    }
  }
}
