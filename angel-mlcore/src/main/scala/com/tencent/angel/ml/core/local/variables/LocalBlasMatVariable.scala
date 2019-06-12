package com.tencent.angel.ml.core.local.variables

import java.util.Random

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.EnvContext
import com.tencent.angel.ml.core.utils.{OptUtils, ValueNotAllowed}
import com.tencent.angel.ml.core.variable.{BlasMatVariable, Updater, VariableManager}
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.storage.{IntDoubleDenseVectorStorage, IntFloatDenseVectorStorage}
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.math2.{MFactory, StorageType}


class LocalBlasMatVariable(name: String,
                           val numRows: Int,
                           val numCols: Long,
                           updater: Updater,
                           rowType: RowType,
                           formatClassName: String,
                           allowPullWithIndex: Boolean)
                          (implicit variableManager: VariableManager)
  extends LocalVariable(name, rowType, updater, formatClassName, allowPullWithIndex) with BlasMatVariable {
  override protected var matrix: Matrix = _

  protected override def doCreate[T](envCtx: EnvContext[T]): Unit = {
    assert(envCtx == null || envCtx.client == null)
    storage = SharedConf.valueType() match {
      case "float" =>
        MFactory.rbIntFloatMatrix(numSlot + 1, (numRows * numCols).toInt, StorageType.DENSE)
      case "double" =>
        MFactory.rbIntDoubleMatrix(numSlot + 1, (numRows * numCols).toInt, StorageType.DENSE)
      case _ => throw ValueNotAllowed("Value Not Allowed, Only Float/Double Are Allowed!")
    }
  }

  protected override def doInit(taskFlag: Int): Unit = {
    if (taskFlag == 0 && rowType.isDense) {
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
  }

  protected override def doPull(epoch: Int, indices: Vector = null): Unit = {
    if (matrix == null) {
      matrix = storage.getRow(0).getStorage match {
        case s: IntDoubleDenseVectorStorage =>
          MFactory.denseDoubleMatrix(numRows, numCols.toInt, s.getValues)
        case s: IntFloatDenseVectorStorage =>
          MFactory.denseFloatMatrix(numRows, numCols.toInt, s.getValues)
        case _ => throw ValueNotAllowed("Value Not Allowed, Only Float/Double Are Allowed!")
      }
    }
  }

  protected override def doPush(grad: Matrix, alpha: Double): Unit = {
    if (numSlot == 0) {
      OptUtils.getRowAsMatrix(storage, numSlot, numRows, numCols.toInt).isub(grad.imul(alpha))
    } else {
      OptUtils.getRowAsMatrix(storage, numSlot, numRows, numCols.toInt).iadd(grad)
    }
  }
}
