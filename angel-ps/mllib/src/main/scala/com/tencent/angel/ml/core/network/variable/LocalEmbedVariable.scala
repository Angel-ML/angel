package com.tencent.angel.ml.core.network.variable


import com.tencent.angel.ml.core.network.graph.Graph

import com.tencent.angel.ml.math2.storage._
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.matrix.RowType


import java.lang.{Long => JLong}
import java.util.{HashMap => JHashMap, Map => JMap}

class LocalEmbedVariable(name: String, numRows: Int, numCols: Long, numSlot: Int, rowType: RowType)(
  implicit graph: Graph) extends LocalMatVariable(name, numRows, numCols, numSlot, rowType)(graph) with MatVariable {
  private val embeddings: JMap[JLong, Vector] = new JHashMap[JLong, Vector]()

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
