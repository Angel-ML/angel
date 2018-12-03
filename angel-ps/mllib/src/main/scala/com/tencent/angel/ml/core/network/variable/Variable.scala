package com.tencent.angel.ml.core.network.variable

import Variable.Location._
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.core.network.graph.Graph
import com.tencent.angel.ml.core.network.variable.MatVariable.MatrixType
import com.tencent.angel.ml.core.utils.RowTypeUtils


abstract class Variable(val name: String, val rowType: RowType, val location: Location)(implicit graph: Graph) {
  graph.addVariable(this)

  def storageType: String = RowTypeUtils.storageType(rowType)

  def valueType: String = RowTypeUtils.valueType(rowType)

  def keyType: String = RowTypeUtils.keyType(rowType)
}

object Variable {
  def getVector(name: String, length: Long, numSlot: Int, rowType: RowType,
                location: Location)(implicit graph: Graph): VecVariable = {
    VecVariable(name, length, numSlot, rowType, location)(graph)
  }

  def getVector(name: String, numCols: Long, rowType: RowType,
                location: Location)(implicit graph: Graph): VecVariable = {
    VecVariable(name, numCols, 0, rowType, location)(graph)
  }

  def getMatrix(name: String, numRows: Int, numCols: Long, numSlot: Int, rowType: RowType,
                matType: MatrixType.MatrixType, location: Location)(implicit graph: Graph): MatVariable = {
    MatVariable(name, numRows, numCols, 0, numSlot, rowType, matType, location)(graph)
  }

  def getMatrix(name: String, numRows: Int, numCols: Long, validIndexNum: Long, numSlot: Int, rowType: RowType,
                matType: MatrixType.MatrixType, location: Location)(implicit graph: Graph): MatVariable = {
    MatVariable(name, numRows, numCols, validIndexNum, numSlot, rowType, matType, location)(graph)
  }

  object Location extends Enumeration {
    type Location = Value
    val PS, Local = Value
  }

}
