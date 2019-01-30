package com.tencent.angel.ml.core.local.variables

import java.util.Random

import com.tencent.angel.ml.core.network.{EvnContext, Graph}
import com.tencent.angel.ml.core.utils.ValueNotAllowed
import com.tencent.angel.ml.core.variable.{Updater, VarState, VecVariable}
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.storage._
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.math2.{MFactory, StorageType}


class LocalVecVariable(name: String, val length: Long, updater: Updater,
                       rowType: RowType, formatClassName: String, allowPullWithIndex: Boolean)(implicit graph: Graph)
  extends LocalVariable(name, rowType, updater, formatClassName, allowPullWithIndex) with VecVariable {
  override protected var vector: Vector = _

  protected override def doCreate(envCtx: EvnContext): Unit = {
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

  protected override def doInit(taskFlag: Int): Unit = {
    if (taskFlag == 0 && rowType.isDense) {
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
  }

  protected override def doPull(epoch: Int, indices: Vector = null): Unit = {
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
  }

  protected override def doPush(grad: Matrix, alpha: Double): Unit = {
    if (numSlot == 0) {
      storage.getRow(0).isub(grad.getRow(0).imul(alpha))
    } else {
      storage.getRow(numSlot).iadd(grad.getRow(0))
    }
  }
}
