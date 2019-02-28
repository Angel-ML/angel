package com.tencent.angel.ml.core.local.variables

import java.lang.{Long => JLong}
import java.util.{HashMap => JHashMap, Map => JMap}

import com.tencent.angel.ml.core.network.layers.PlaceHolder
import com.tencent.angel.ml.core.utils.ValueNotAllowed
import com.tencent.angel.ml.core.variable.{EmbedUtils, EmbedVariable, Updater, VariableManager}
import com.tencent.angel.ml.math2.matrix.{MapMatrix, Matrix}
import com.tencent.angel.ml.math2.storage._
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector.Vector


class LocalEmbedVariable(name: String,
                         numRows: Int,
                         numCols: Long,
                         updater: Updater,
                         rowType: RowType,
                         formatClassName: String,
                         allowPullWithIndex: Boolean,
                         placeHolder: PlaceHolder)
                        (implicit variableManager: VariableManager)
  extends LocalMatVariable(name, numRows, numCols, updater, rowType, formatClassName, allowPullWithIndex) with EmbedVariable {
  private val embeddings: JMap[JLong, Vector] = new JHashMap[JLong, Vector]()

  protected override def doPull(epoch: Int, indices: Vector = null): Unit = {
    embeddings.clear()

    if (indices != null) {
      indices.getStorage match {
        case s: IntIntDenseVectorStorage =>
          s.getValues.foreach { idx => embeddings.put(idx.toLong, storage.getRow(idx)) }
        case s: IntLongDenseVectorStorage =>
          s.getValues.foreach { idx => embeddings.put(idx, storage.getRow(idx.toInt)) }
      }
    } else {
      (0 until numRows).foreach { idx =>
        embeddings.put(idx.toLong, storage.getRow(idx))
      }
    }

    matrix = EmbedUtils.geneMatrix(placeHolder.getFeats, embeddings)
  }

  protected override def doPush(grad: Matrix, alpha: Double): Unit = {
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
  }
}
