package com.tencent.angel.ml.core.network.variable

import java.util.Random
import java.util.concurrent.Future

import com.tencent.angel.ml.core.network.graph.Graph
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.ValueNotAllowed
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.storage._
import com.tencent.angel.ml.math2.{MFactory, StorageType}
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult


class LocalMatVariable(name: String, numRows: Int, numCols: Long, numSlot: Int, rowType: RowType)(implicit graph: Graph)
  extends LocalVariable(name, rowType)(graph) with MatVariable {
  override protected var matrix: Matrix = _
  override protected var rowsSaved: Array[Int] = if (numSlot > 0) (0 to numSlot).toArray else Array(0)

  protected var mean: Double = 0
  protected var stddev: Double = 0.000001

  override def create(): Unit = {
    if (storage == null) {
      storage = rowType match {
        case RowType.T_DOUBLE_DENSE =>
          val sto = MFactory.rbIntDoubleMatrix((numSlot + 1) * numRows, numCols.toInt, StorageType.DENSE)
          val rows = (0 until numRows).toArray.map { rId => sto.getRow(rId) }
          matrix = MFactory.rbIntDoubleMatrix(rows)
          sto
        case RowType.T_DOUBLE_SPARSE =>
          val sto = MFactory.rbIntDoubleMatrix((numSlot + 1) * numRows, numCols.toInt, StorageType.SPARSE)
          val rows = (0 until numRows).toArray.map { rId => sto.getRow(rId) }
          matrix = MFactory.rbIntDoubleMatrix(rows)
          sto
        case RowType.T_DOUBLE_SPARSE_LONGKEY =>
          val sto = MFactory.rbLongDoubleMatrix((numSlot + 1) * numRows, numCols, StorageType.SPARSE)
          val rows = (0 until numRows).toArray.map { rId => sto.getRow(rId) }
          matrix = MFactory.rbLongDoubleMatrix(rows)
          sto
        case RowType.T_FLOAT_DENSE =>
          val sto = MFactory.rbIntFloatMatrix((numSlot + 1) * numRows, numCols.toInt, StorageType.DENSE)
          val rows = (0 until numRows).toArray.map { rId => sto.getRow(rId) }
          matrix = MFactory.rbIntFloatMatrix(rows)
          sto
        case RowType.T_FLOAT_SPARSE =>
          val sto = MFactory.rbIntFloatMatrix((numSlot + 1) * numRows, numCols.toInt, StorageType.SPARSE)
          val rows = (0 until numRows).toArray.map { rId => sto.getRow(rId) }
          matrix = MFactory.rbIntFloatMatrix(rows)
          sto
        case RowType.T_FLOAT_SPARSE_LONGKEY =>
          val sto = MFactory.rbLongFloatMatrix((numSlot + 1) * numRows, numCols, StorageType.SPARSE)
          val rows = (0 until numRows).toArray.map { rId => sto.getRow(rId) }
          matrix = MFactory.rbLongFloatMatrix(rows)
          sto
        case _ => throw ValueNotAllowed("Value Not Allowed, Only Float/Double Are Allowed!")
      }
    }
  }

  override def init(taskFlag: Int, mean: Double, stddev: Double): Unit = {
    this.mean = mean
    this.stddev = stddev

    val random = new Random()

    (0 until numRows).foreach { rId =>
      matrix.getRow(rId).getStorage match {
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

  override def pullParams(epoch: Int, indices: Vector = null): Unit = {
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


    if (epoch == 0 && indices != null) {
      val random = new Random()
      (0 until numRows).foreach { rId =>
        matrix.getRow(rId).getStorage match {
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
  }

  override def pushGrads(features: Matrix, backward: Matrix): Unit = ???

  override def update(optimizer: Optimizer, epoch: Int, batchSize: Int): Future[VoidResult] = ???

}
