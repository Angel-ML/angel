package com.tencent.angel.ml.core.local.variables

import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.variable.Variable
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.utils.RowType


abstract class LocalVariable(name: String, rowType: RowType)(implicit graph: Graph)
  extends Variable(name, rowType) {
  var storage: Matrix = _

  def create(): Unit
}
