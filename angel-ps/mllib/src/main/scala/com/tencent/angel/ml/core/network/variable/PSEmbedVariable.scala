package com.tencent.angel.ml.core.network.variable

import com.tencent.angel.ml.core.network.graph.Graph
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.storage._
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.psf.columns._
import com.tencent.angel.ps.server.data.request.RandomNormalInitFunc
import com.tencent.angel.psagent.PSAgentContext
import java.lang.{Long => JLong}
import java.util.{HashMap => JHashMap, Map => JMap}

import com.tencent.angel.ml.core.utils.PSMatrixUtils


class PSEmbedVariable(name: String, numRows: Int, numCols: Long, validIndexNum: Long, numSlot: Int,
                      rowType: RowType)(implicit graph: Graph)
  extends PSMatVariable(name, numRows, numCols, validIndexNum, numSlot, rowType) with MatVariable {
  private var embeddings: JMap[JLong, Vector] = _

  override def pullParams(epoch: Int, indices: Vector): Unit = {
    if (matrixId == -1) {
      matrixId = PSMatrixUtils.getMatrixId(name)
    }

    // step 1: pull embedding
    val param = if (epoch == 0) {
      val initFunc = new RandomNormalInitFunc(mean, stddev)
      new GetColsParam(matrixId, (0 until numRows).toArray, indices, initFunc)
    } else {
      new GetColsParam(matrixId, (0 until numRows).toArray, indices)
    }

    val func = new GetColsFunc(param)
    val result = PSAgentContext.get.getUserRequestAdapter.get(func).asInstanceOf[GetColsResult]
    embeddings = result.results

    // step 2: generate matrix
    assert(embeddings != null)
    matrix = EmbedUtils.geneMatrix(graph, embeddings)
  }

  private def mergeUpdate(map: JMap[JLong, Vector], key: Long, update: Vector, value: Double): Unit = {
    if (!map.containsKey(key)) {
      if (value == 1) map.put(key, update)
      else map.put(key, update.imul(value))
    } else {
      if (value == 1) map.get(key).iadd(update)
      else map.get(key).iadd(update.imul(value))
    }
  }

  override def pushGrads(features: Matrix, backward: Matrix): Unit = {
    val map: JMap[JLong, Vector] = new JHashMap()
    val batchSize = graph.placeHolder.getBatchSize
    assert(backward.getNumRows == graph.placeHolder.getBatchSize)

    (0 until batchSize).foreach { idx =>
      val feat = features.getRow(idx)
      val grad = backward.getRow(idx)
      (feat.getStorage, grad) match {
        case (s: IntDoubleDenseVectorStorage, g: CompIntDoubleVector) =>
          s.getValues.zipWithIndex.foreach { case (value, i) =>
            mergeUpdate(map, i, g.getPartitions()(i), value)
          }
        case (s: IntDoubleSparseVectorStorage, g: CompIntDoubleVector) =>
          s.getIndices.sorted.zipWithIndex.foreach { case (key, i) =>
            mergeUpdate(map, key, g.getPartitions()(i), s.get(key))
          }
        case (s: IntDoubleSortedVectorStorage, g: CompIntDoubleVector) =>
          s.getIndices.zip(s.getValues).zipWithIndex.foreach { case ((key, value), i) =>
            mergeUpdate(map, key, g.getPartitions()(i), value)
          }
        case (s: IntFloatDenseVectorStorage, g: CompIntFloatVector) =>
          s.getValues.zipWithIndex.foreach { case (value, i) =>
            mergeUpdate(map, i, g.getPartitions()(i), value)
          }
        case (s: IntFloatSparseVectorStorage, g: CompIntFloatVector) =>
          s.getIndices.sorted.zipWithIndex.foreach { case (key, i) =>
            mergeUpdate(map, key, g.getPartitions()(i), s.get(key))
          }
        case (s: IntFloatSortedVectorStorage, g: CompIntFloatVector) =>
          s.getIndices.zip(s.getValues).zipWithIndex.foreach { case ((key, value), i) =>
            mergeUpdate(map, key, g.getPartitions()(i), value)
          }
        case (s: LongDoubleSparseVectorStorage, g: CompIntDoubleVector) =>
          s.getIndices.sorted.zipWithIndex.foreach { case (key, i) =>
            mergeUpdate(map, key, g.getPartitions()(i), s.get(key))
          }
        case (s: LongDoubleSortedVectorStorage, g: CompIntDoubleVector) =>
          s.getIndices.zip(s.getValues).zipWithIndex.foreach { case ((key, value), i) =>
            mergeUpdate(map, key, g.getPartitions()(i), value)
          }
        case (s: LongFloatSparseVectorStorage, g: CompIntFloatVector) =>
          s.getIndices.sorted.zipWithIndex.foreach { case (key, i) =>
            mergeUpdate(map, key, g.getPartitions()(i), s.get(key))
          }
        case (s: LongFloatSortedVectorStorage, g: CompIntFloatVector) =>
          s.getIndices.zip(s.getValues).zipWithIndex.foreach { case ((key, value), i) =>
            mergeUpdate(map, key, g.getPartitions()(i), value)
          }
        case _ =>
      }
    }

    // Divide Gradient with TaskNum*BatchSize
    val iter = map.values().iterator()
    while (iter.hasNext) {
      val vector = iter.next()
      vector.idiv(normal)
    }

    // Push Gradient
    val start = numRows * numSlot
    val end = numRows * (numSlot + 1)

    val param = new UpdateColsParam(matrixId, (start until end).toArray, graph.placeHolder.getIndices, map)
    val func = new UpdateColsFunc(param)
    PSAgentContext.get().getUserRequestAdapter.update(func).get()
  }

}
