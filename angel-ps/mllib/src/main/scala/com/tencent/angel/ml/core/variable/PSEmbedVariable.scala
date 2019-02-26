package com.tencent.angel.ml.core.variable

import java.lang.{Long => JLong}
import java.util.{Map => JMap}

import com.tencent.angel.matrix.psf.update.RandomNormal
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.math2.matrix.{MapMatrix, Matrix}
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.psf.columns._
import com.tencent.angel.ps.server.data.request.RandomNormalInitFunc
import com.tencent.angel.psagent.PSAgentContext


class PSEmbedVariable(name: String, numRows: Int, numCols: Long, validIndexNum: Long,
                      updater: Updater, rowType: RowType, formatClassName: String,
                      allowPullWithIndex: Boolean)(implicit graph: Graph)
  extends PSMatVariable(name, numRows, numCols, validIndexNum, updater, rowType, formatClassName,
    allowPullWithIndex) with EmbedVariable {
  private var embeddings: JMap[JLong, Vector] = _

  protected override def doPull(epoch: Int, indices: Vector = null): Unit = {
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

    matrix = EmbedUtils.geneMatrix(graph, embeddings)
  }

  protected override def doPush(grad: Matrix, alpha: Double): Unit = {
    val map: JMap[JLong, Vector] = grad.asInstanceOf[MapMatrix[Vector]].getMap

    // Divide Gradient with TaskNum*BatchSize
    val iter = map.values().iterator()
    while (iter.hasNext) {
      val vector = iter.next()
      if (numSlot == 0) {
        vector.imul(-normal * alpha)
      } else {
        vector.imul(normal)
      }
    }

    // Push Gradient
    val start = numRows * numSlot
    val end = numRows * (numSlot + 1)

    val param = new UpdateColsParam(matrixId, (start until end).toArray, graph.placeHolder.getIndices, map)
    val func = new UpdateColsFunc(param)
    PSAgentContext.get().getUserRequestAdapter.update(func).get()
  }
}
