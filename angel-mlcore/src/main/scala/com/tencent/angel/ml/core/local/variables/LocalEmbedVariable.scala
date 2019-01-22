package com.tencent.angel.ml.core.local.variables

import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.variable.{EmbedUtils, EmbedVariable, Updater, VarState}
import com.tencent.angel.ml.math2.matrix.{MapMatrix, Matrix}
import com.tencent.angel.ml.math2.storage._
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector.Vector


import com.tencent.angel.ml.core.utils.ValueNotAllowed

class LocalEmbedVariable(name: String, numRows: Int, numCols: Long, updater: Updater,
                         rowType: RowType, withInput: Boolean)(implicit graph: Graph)
  extends LocalMatVariable(name, numRows, numCols, updater, rowType, withInput) with EmbedVariable {

  override def pull(epoch: Int, indices: Vector = null): Unit = {
    writeLock.lock()

    try {
      assert(indices != null)
      if (state == VarState.Initialized || state == VarState.Ready) {
        embeddings.clear()

        indices.getStorage match {
          case s: IntIntDenseVectorStorage =>
            s.getValues.foreach { idx => embeddings.put(idx.toLong, storage.getRow(idx)) }
          case s: IntLongDenseVectorStorage =>
            s.getValues.foreach { idx => embeddings.put(idx, storage.getRow(idx.toInt)) }
        }

        matrix = EmbedUtils.geneMatrix(graph, embeddings)

        if (state == VarState.Initialized) {
          transSate(VarState.Initialized, VarState.Ready)
        }
      }

      assert(state == VarState.Ready)
    } finally {
      writeLock.unlock()
    }
  }

  override def push(grad: Matrix, alpha: Double): Unit = {
    writeLock.lock()

    try {
      assert(state == VarState.Ready)

      if (numSlot == 0) {
        grad match {
          case mat: MapMatrix[_] =>
            val tempMap = mat.getMap
            val iter = tempMap.keySet().iterator()
            while (iter.hasNext) {
              val key = iter.next()
              val vector = tempMap.get(key)

              storage.getRow(key.toInt).isub(vector.imul(alpha))
            }
          case _ => throw ValueNotAllowed("Only MapMatrix is allow as a gradient matrix!")
        }
      } else {
        grad match {
          case mat: MapMatrix[_] =>
            val tempMap = mat.getMap
            val iter = tempMap.keySet().iterator()
            val start = numRows * numSlot
            while (iter.hasNext) {
              val key = iter.next()
              val vector = tempMap.get(key)

              storage.getRow(start + key.toInt).iadd(vector)
            }
          case _ => throw ValueNotAllowed("Only MapMatrix is allow as a gradient matrix!")
        }
      }
    } finally {
      writeLock.unlock()
    }

  }
}
