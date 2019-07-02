package com.tencent.angel.ml.core.local.variables

import java.util.Random

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.EnvContext
import com.tencent.angel.ml.core.utils.{OptUtils, ValueNotAllowed}
import com.tencent.angel.ml.core.variable.{MatVariable, Updater, VariableManager}
import com.tencent.angel.ml.servingmath2.matrix.Matrix
import com.tencent.angel.ml.servingmath2.storage._
import com.tencent.angel.ml.servingmath2.utils.RowType
import com.tencent.angel.ml.servingmath2.vector._
import com.tencent.angel.ml.servingmath2.{MFactory, StorageType}


class LocalMatVariable(name: String,
                       val numRows: Int,
                       val numCols: Long,
                       updater: Updater,
                       rowType: RowType,
                       formatClassName: String,
                       allowPullWithIndex: Boolean)
                      (implicit  conf: SharedConf, variableManager: VariableManager)
  extends LocalVariable(name, rowType, updater, formatClassName, allowPullWithIndex) with MatVariable {
  override protected var matrix: Matrix = _

  protected override def doCreate[T](envCtx: EnvContext[T]): Unit = {
    assert(envCtx == null || envCtx.client == null)
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
  }

  protected override def doInit(taskFlag: Int): Unit = {
    if (taskFlag == 0 && rowType.isDense) {
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
  }

  protected override def doPull(epoch: Int, indices: Vector = null): Unit = {
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
  }

  protected override def doPush(grad: Matrix, alpha: Double): Unit = {
    if (numSlot == 0) {
      OptUtils.getRowsAsMatrix(storage, 0, numRows).isub(grad.imul(alpha))
    } else {
      OptUtils.getRowsAsMatrix(storage, numRows * numSlot, numRows * (numSlot + 1)).iadd(grad)
    }
  }
}
