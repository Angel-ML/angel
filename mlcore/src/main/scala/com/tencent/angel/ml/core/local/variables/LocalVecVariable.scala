package com.tencent.angel.ml.core.local.variables

import java.util.Random
import java.util.concurrent.Future

import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.variable.VecVariable
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.ValueNotAllowed
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.storage._
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.math2.{MFactory, StorageType}


class LocalVecVariable(name: String, length: Long, numSlot: Int, rowType: RowType)(implicit graph: Graph)
  extends LocalVariable(name, rowType)(graph) with VecVariable {
  override protected var vector: Vector = _
  // override protected val rowsSaved: Array[Int] = Array(0)

  private var mean: Double = 0
  private var stddev: Double = 0.000001

  def create(): Unit = {
    if (storage == null) {
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

      vector = storage.getRow(0)
    }
  }

  def init(taskFlag: Int, mean: Double, stddev: Double): Unit = {
    this.mean = mean
    this.stddev = stddev

    if (taskFlag == 0) {
      val random = new Random()
      vector.getStorage match {
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

  def pullParams(epoch: Int, indices: Vector = null): Unit = {
    if (vector == null) {
      vector = storage.getRow(0)
    }

    if (epoch == 0 && indices != null) {
      val random = new Random()
      vector.getStorage match {
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
              storage.set(i, random.nextDouble() * 0.00001 * stddev + mean)
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

  def pushGrads(backward: Matrix, lr: Double): Unit = ???

  def pushGrads(grad: Vector, lr: Double): Unit = ???

  def pushGrads(features: Matrix, backward: Matrix): Unit = ???

  override def update[T](optimizer: Optimizer, epoch: Int, batchSize: Int): Future[T] = ???

}
