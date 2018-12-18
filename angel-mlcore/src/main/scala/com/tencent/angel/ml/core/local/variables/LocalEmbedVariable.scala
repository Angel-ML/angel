package com.tencent.angel.ml.core.local.variables

import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.variable.{EmbedUtils, EmbedVariable}
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.storage._
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector.Vector
import java.lang.{Long => JLong}
import java.util.{Map => JMap}
import java.util

import com.tencent.angel.ml.core.utils.MLException

class LocalEmbedVariable(name: String, numRows: Int, numCols: Long, numSlot: Int, rowType: RowType)(
  implicit graph: Graph) extends LocalMatVariable(name, numRows, numCols, numSlot, rowType) with EmbedVariable {

  override def pullParams(epoch: Int, indices: Vector = null): Unit = {
    assert(indices != null)
    embeddings.clear()
    indices.getStorage match {
      case s: IntIntDenseVectorStorage =>
        s.getValues.foreach { idx => embeddings.put(idx.toLong, storage.getRow(idx)) }
      case s: IntLongDenseVectorStorage =>
        s.getValues.foreach { idx => embeddings.put(idx, storage.getRow(idx.toInt)) }
    }

    matrix = EmbedUtils.geneMatrix(graph, embeddings)
  }

  override def pushGrads(features: Matrix, backward: Matrix): Unit = {
    val tempMap = new util.HashMap[JLong, Vector]()

    (0 until features.getNumRows).foreach { rId =>
      val row = features.getRow(rId)
      val partitions = EmbedUtils.getPartitions(backward, rId)

      row.getStorage match {
        case s: IntDoubleSparseVectorStorage =>
          val iter = s.entryIterator()
          var id: Int = 0
          while (iter.hasNext) {
            val entry = iter.next()
            val key = entry.getIntKey
            val value = entry.getDoubleValue
            val update = partitions(id)
            EmbedUtils.mergeUpdate(tempMap, key, update, value)
            id += 1
          }
        case s: IntFloatSparseVectorStorage =>
          val iter = s.entryIterator()
          var id: Int = 0
          while (iter.hasNext) {
            val entry = iter.next()
            val key = entry.getIntKey
            val value = entry.getFloatValue
            val update = partitions(id)
            EmbedUtils.mergeUpdate(tempMap, key, update, value)
            id += 1
          }
        case s: LongDoubleSparseVectorStorage =>
          val iter = s.entryIterator()
          var id: Int = 0
          while (iter.hasNext) {
            val entry = iter.next()
            val key = entry.getLongKey
            val value = entry.getDoubleValue
            val update = partitions(id)
            EmbedUtils.mergeUpdate(tempMap, key, update, value)
            id += 1
          }
        case s: LongFloatSparseVectorStorage =>
          val iter = s.entryIterator()
          var id: Int = 0
          while (iter.hasNext) {
            val entry = iter.next()
            val key = entry.getLongKey
            val value = entry.getFloatValue
            val update = partitions(id)
            EmbedUtils.mergeUpdate(tempMap, key, update, value)
            id += 1
          }
        //        case s: IntDoubleSortedVectorStorage =>
        //          s.getValues.zip(s.getIndices).zipWithIndex{ case ((value, key), id) =>
        //            val update = partitions(id)
        //            EmbedUtils.mergeUpdate(tempMap, key, update, value)
        //          }
        //        case s: IntFloatSortedVectorStorage =>
        //          s.getValues.zip(s.getIndices).zipWithIndex{ case ((value, key), id) =>
        //            val update = partitions(id)
        //            EmbedUtils.mergeUpdate(tempMap, key, update, value)
        //          }
        //        case s: LongDoubleSortedVectorStorage =>
        //          s.getValues.zip(s.getIndices).zipWithIndex{ case ((value, key), id) =>
        //            val update = partitions(id)
        //            EmbedUtils.mergeUpdate(tempMap, key, update, value)
        //          }
        //        case s: LongFloatSortedVectorStorage =>
        //          s.getValues.zip(s.getIndices).zipWithIndex{ case ((value, key), id) =>
        //            val update = partitions(id)
        //            EmbedUtils.mergeUpdate(tempMap, key, update, value)
        //          }
        //        case s: IntDoubleDenseVectorStorage =>
        //          s.getValues.zipWithIndex{ case (value, key) =>
        //            val update = partitions(key)
        //            EmbedUtils.mergeUpdate(tempMap, key, update, value)
        //          }
        //        case s: IntFloatDenseVectorStorage =>
        //          s.getValues.zipWithIndex{ case (value, key) =>
        //            val update = partitions(key)
        //            EmbedUtils.mergeUpdate(tempMap, key, update, value)
        //          }
        case _ => throw MLException("Data type is not support!")
      }
    }

    val iter = tempMap.keySet().iterator()
    val start = numRows * numSlot
    while (iter.hasNext) {
      val key = iter.next()
      val vector = tempMap.get(key)

      storage.getRow(start + key.toInt).iadd(vector.imul(graph.normalFactor))
    }
  }
}
