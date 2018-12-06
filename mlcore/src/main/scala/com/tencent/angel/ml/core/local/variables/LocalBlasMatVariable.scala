package com.tencent.angel.ml.core.local.variables

import java.util.Random
import java.util.concurrent.Future

import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.variable.BlasMatVariable
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.ValueNotAllowed
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.storage.{IntDoubleDenseVectorStorage, IntFloatDenseVectorStorage}
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.{MFactory, StorageType, vector}


class LocalBlasMatVariable(name: String, numRows: Int, numCols: Long, numSlot: Int, rowType: RowType)(
  implicit graph: Graph) extends LocalVariable(name, rowType) with BlasMatVariable {
  override protected var matrix: Matrix = _

  override def create(): Unit = {
    if (storage == null) {
      storage = graph.valueType match {
        case "float" =>
          val sto = MFactory.rbIntFloatMatrix(numSlot + 1, (numRows * numCols).toInt, StorageType.DENSE)
          val values = sto.getRow(0).getStorage.asInstanceOf[IntFloatDenseVectorStorage].getValues
          matrix = MFactory.denseFloatMatrix(numRows, numCols.toInt, values)
          sto
        case "double" =>
          val sto = MFactory.rbIntDoubleMatrix(numSlot + 1, (numRows * numCols).toInt, StorageType.DENSE)
          val values = sto.getRow(0).getStorage.asInstanceOf[IntDoubleDenseVectorStorage].getValues
          matrix = MFactory.denseDoubleMatrix(numRows, numCols.toInt, values)
          sto
        case _ => throw ValueNotAllowed("Value Not Allowed, Only Float/Double Are Allowed!")
      }
    }
  }

  override def init(taskFlag: Int, mean: Double, stddev: Double): Unit = {

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
  }

  override def pullParams(epoch: Int, indices: vector.Vector = null): Unit = {
    assert(indices == null)
    if (matrix == null) {
      storage.getRow(0).getStorage match {
        case s: IntDoubleDenseVectorStorage =>
          matrix = MFactory.denseDoubleMatrix(numRows, numCols.toInt, s.getValues)
        case s: IntFloatDenseVectorStorage =>
          matrix = MFactory.denseFloatMatrix(numRows, numCols.toInt, s.getValues)
        case _ => throw ValueNotAllowed("Value Not Allowed, Only Float/Double Are Allowed!")
      }
    }
  }

  override def pushGrads(features: Matrix, backward: Matrix): Unit = ???

  override def pushGrads(grad: Matrix, lr: Double): Unit = ???

  override def update[T](optimizer: Optimizer, epoch: Int, batchSize: Int): Future[T] = ???
}
