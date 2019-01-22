package com.tencent.angel.ml.core.local.variables

import java.util.Random
import java.util.concurrent.Future

import com.tencent.angel.ml.core.network.{EvnContext, Graph}
import com.tencent.angel.ml.core.network.variable.{Updater, VarState, VecVariable}
import com.tencent.angel.ml.core.utils.ValueNotAllowed
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.storage._
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.math2.{MFactory, StorageType}


class LocalVecVariable(name: String, val length: Long, updater: Updater,
                       rowType: RowType, withInput: Boolean)(implicit graph: Graph)
  extends LocalVariable(name, rowType, updater, withInput) with VecVariable {
  override protected var vector: Vector = _
  // override protected val rowsSaved: Array[Int] = Array(0)

  private var mean: Double = 0
  private var stddev: Double = 0.000001

  override def create(envCtx: EvnContext): Unit = {
    writeLock.lock()
    try {
      if (state == VarState.New || state == VarState.Expired) {
        storage = rowType match {
          case RowType.T_DOUBLE_DENSE =>
            MFactory.rbIntDoubleMatrix(numSlot + 1, length.toInt, StorageType.DENSE)
          case RowType.T_DOUBLE_SPARSE =>
            MFactory.rbIntDoubleMatrix(numSlot + 1, length.toInt, StorageType.SPARSE)
          case RowType.T_DOUBLE_SPARSE_LONGKEY =>
            MFactory.rbLongDoubleMatrix(numSlot + 1, length, StorageType.SPARSE)
          case RowType.T_FLOAT_DENSE =>
            MFactory.rbIntFloatMatrix(numSlot + 1, length.toInt, StorageType.DENSE)
          case RowType.T_FLOAT_SPARSE =>
            MFactory.rbIntFloatMatrix(numSlot + 1, length.toInt, StorageType.SPARSE)
          case RowType.T_FLOAT_SPARSE_LONGKEY =>
            MFactory.rbLongFloatMatrix(numSlot + 1, length, StorageType.SPARSE)
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

  override def init(taskFlag: Int, mean: Double, stddev: Double): Unit = {
    writeLock.lock()

    this.mean = mean
    this.stddev = stddev
    try {
      if (state == VarState.Created) {
        if (taskFlag == 0) {
          val random = new Random()
          storage.getRow(0).getStorage match {
            case s: IntDoubleDenseVectorStorage =>
              val values = s.getValues
              (0 until values.size).foreach { idx =>
                values(idx) = random.nextDouble() * stddev + mean
              }
            case s: IntFloatDenseVectorStorage =>
              val values = s.getValues
              val fstddev = stddev.toFloat
              val fmean = mean.toFloat
              (0 until values.size).foreach { idx =>
                values(idx) = random.nextFloat() * fstddev + fmean
              }
            case _ =>
          }
        }

        // trans state
        transSate(VarState.Created, VarState.Initialized)
      }

      assert(state == VarState.Initialized)
    } finally {
      writeLock.unlock()
    }
  }

  override def pull(epoch: Int, indices: Vector = null): Unit = {
    writeLock.lock()
    try {
      if (state == VarState.Initialized || state == VarState.Ready) {
        if (epoch == 0 && indices != null && rowType.isSparse) {
          val random = new Random()
          storage.getRow(0).getStorage match {
            case storage: IntDoubleSparseVectorStorage =>
              val idxs = indices.getStorage.asInstanceOf[IntIntDenseVectorStorage].getValues
              idxs.foreach { i =>
                if (!storage.hasKey(i)) {
                  storage.set(i, random.nextDouble() * stddev + mean)
                }
              }
            case storage: LongDoubleSparseVectorStorage =>
              val idxs = indices.getStorage.asInstanceOf[IntLongDenseVectorStorage].getValues
              idxs.foreach { i =>
                if (!storage.hasKey(i)) {
                  storage.set(i, random.nextDouble() * stddev + mean)
                }
              }
            case storage: IntFloatSparseVectorStorage =>
              val idxs = indices.getStorage.asInstanceOf[IntIntDenseVectorStorage].getValues
              idxs.foreach { i =>
                if (!storage.hasKey(i)) {
                  storage.set(i, (random.nextDouble() * stddev + mean).toFloat)
                }
              }
            case storage: LongFloatSparseVectorStorage =>
              val idxs = indices.getStorage.asInstanceOf[IntLongDenseVectorStorage].getValues
              idxs.foreach { i =>
                if (!storage.hasKey(i)) {
                  storage.set(i, (random.nextDouble() * stddev + mean).toFloat)
                }
              }
            case _ =>
          }
        }

        if (vector == null) {
          vector = storage.getRow(0)
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

  def push(grad: Matrix, alpha: Double): Unit = {
    writeLock.lock()

    try {
      assert(state == VarState.Ready)

      if (numSlot == 0) {
        storage.getRow(0).isub(grad.getRow(0).imul(alpha))
      } else {
        storage.getRow(numSlot).iadd(grad.getRow(0))
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
