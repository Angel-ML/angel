package com.tencent.angel.ml.core.local.variables

import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.variable.{EmbedUtils, EmbedVariable}
import com.tencent.angel.ml.math2.storage._
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector.Vector

class LocalEmbedVariable(name: String, numRows: Int, numCols: Long, numSlot: Int, rowType: RowType)(
  implicit graph: Graph) extends LocalMatVariable(name, numRows, numCols, numSlot, rowType)(graph) with EmbedVariable {

  override def pullParams(epoch: Int, indices: Vector = null): Unit = {
    assert(indices != null)
    embeddings.clear()
    indices.getStorage match {
      case s: IntIntDenseVectorStorage =>
        s.getValues.foreach{ idx => embeddings.put(idx.toLong, storage.getRow(idx)) }
      case s: IntLongDenseVectorStorage =>
        s.getValues.foreach{ idx => embeddings.put(idx, storage.getRow(idx.toInt)) }
    }

    matrix = EmbedUtils.geneMatrix(graph, embeddings)
  }

}
