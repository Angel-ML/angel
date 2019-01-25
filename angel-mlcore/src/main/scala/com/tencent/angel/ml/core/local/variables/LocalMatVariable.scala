package com.tencent.angel.ml.core.local.variables

import java.util.Random
import java.util.concurrent.Future

import com.tencent.angel.ml.core.network.{EvnContext, Graph}
import com.tencent.angel.ml.core.network.variable.{MatVariable, Updater, VarState}
import com.tencent.angel.ml.core.utils.{OptUtils, ValueNotAllowed}
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.storage._
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.math2.{MFactory, StorageType}


class LocalMatVariable(name: String, val numRows: Int, val numCols: Long, updater: Updater,
                       rowType: RowType, allowPullWithIndex: Boolean)(implicit graph: Graph)
  extends LocalVariable(name, rowType, updater, allowPullWithIndex) with MatVariable {
  override protected var matrix: Matrix = _

  protected var mean: Double = 0
  protected var stddev: Double = 0.000001

  override def create(envCtx: EvnContext): Unit = {
    writeLock.lock()

    try {
      if (state == VarState.New || state == VarState.Expired) {
        storage = rowType match {
          case RowType.T_DOUBLE_DENSE =>
            MFactory.rbIntDoubleMatrix((numSlot + 1) * numRows, numCols.toInt, StorageType.DENSE)
          case RowType.T_DOUBLE_SPARSE =>
            MFactory.rbIntDoubleMatrix((numSlot + 1) * numRows, numCols.toInt, StorageType.SPARSE)
          case RowType.T_DOUBLE_SPARSE_LONGKEY =>
            MFactory.rbLongDoubleMatrix((numSlot + 1) * numRows, numCols, StorageType.SPARSE)
          case RowType.T_FLOAT_DENSE =>
            MFactory.rbIntFloatMatrix((numSlot + 1) * numRows, numCols.toInt, StorageType.DENSE)
          case RowType.T_FLOAT_SPARSE =>
            MFactory.rbIntFloatMatrix((numSlot + 1) * numRows, numCols.toInt, StorageType.SPARSE)
          case RowType.T_FLOAT_SPARSE_LONGKEY =>
            MFactory.rbLongFloatMatrix((numSlot + 1) * numRows, numCols, StorageType.SPARSE)
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
          (0 until numRows).foreach { rId =>
            storage.getRow(rId).getStorage match {
              case storage: IntDoubleDenseVectorStorage =>
                val values = storage.getValues
                values.indices.foreach { idx =>
                  values(idx) = random.nextDouble() * stddev + mean
                }
              case storage: IntFloatDenseVectorStorage =>
                val values = storage.getValues
                values.indices.foreach { idx =>
                  values(idx) = (random.nextDouble() * stddev + mean).toFloat
                }
              case _ =>
            }
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

  override def pull(epoch: Int, indices: Vector = null): Unit = {
    writeLock.lock()

    try {
      if (state == VarState.Initialized || state == VarState.Ready) {
        if (epoch == 0 && indices != null && rowType.isSparse) {
          val random = new Random()
          (0 until numRows).foreach { rId =>
            storage.getRow(rId).getStorage match {
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
        }

        if (matrix == null) {
          matrix = rowType match {
            case RowType.T_DOUBLE_DENSE =>
              val rows = (0 until numRows).toArray.map { rId => storage.getRow(rId).asInstanceOf[IntDoubleVector] }
              MFactory.rbIntDoubleMatrix(rows)
            case RowType.T_DOUBLE_SPARSE =>
              val rows = (0 until numRows).toArray.map { rId => storage.getRow(rId).asInstanceOf[IntDoubleVector] }
              MFactory.rbIntDoubleMatrix(rows)
            case RowType.T_DOUBLE_SPARSE_LONGKEY =>
              val rows = (0 until numRows).toArray.map { rId => storage.getRow(rId).asInstanceOf[LongDoubleVector] }
              MFactory.rbLongDoubleMatrix(rows)
            case RowType.T_FLOAT_DENSE =>
              val rows = (0 until numRows).toArray.map { rId => storage.getRow(rId).asInstanceOf[IntFloatVector] }
              MFactory.rbIntFloatMatrix(rows)
            case RowType.T_FLOAT_SPARSE =>
              val rows = (0 until numRows).toArray.map { rId => storage.getRow(rId).asInstanceOf[IntFloatVector] }
              MFactory.rbIntFloatMatrix(rows)
            case RowType.T_FLOAT_SPARSE_LONGKEY =>
              val rows = (0 until numRows).toArray.map { rId => storage.getRow(rId).asInstanceOf[LongFloatVector] }
              MFactory.rbLongFloatMatrix(rows)
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

      (0 until numRows).toArray.foreach { colId =>
        val grad = features.transDot(backward.getCol(colId)).imul(graph.normalFactor)
        storage.getRow(numSlot * numRows + colId).iadd(grad)
      }
    } finally {
      writeLock.unlock()
    }
  }

  override def push(grad: Matrix, alpha: Double): Unit = {
    writeLock.lock()

    try {
      assert(state == VarState.Ready)
      if (numSlot == 0) {
        OptUtils.getRowsAsMatrix(storage, 0, numRows).isub(grad.imul(alpha))
      } else {
        OptUtils.getRowsAsMatrix(storage, numRows * numSlot, numRows * (numSlot + 1)).iadd(grad)
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
