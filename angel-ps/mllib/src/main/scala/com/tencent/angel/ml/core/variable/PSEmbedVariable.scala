package com.tencent.angel.ml.core.variable

import java.lang.{Long => JLong}
import java.util.{Map => JMap}

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.PlaceHolder
import com.tencent.angel.ml.servingmath2.matrix.{MapMatrix, Matrix}
import com.tencent.angel.ml.servingmath2.utils.RowType
import com.tencent.angel.ml.servingmath2.vector._
import com.tencent.angel.ml.psf.columns._
import com.tencent.angel.ps.server.data.request.RandomNormalInitFunc
import com.tencent.angel.psagent.PSAgentContext


class PSEmbedVariable(name: String,
                      numRows: Int,
                      numCols: Long,
                      validIndexNum: Long,
                      updater: Updater,
                      rowType: RowType,
                      formatClassName: String,
                      allowPullWithIndex: Boolean,
                      taskNum: Int,
                      placeHolder: PlaceHolder)
                     (implicit  conf: SharedConf, variableManager: VariableManager, cilsImpl: CILSImpl)
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

    val matStats = EmbedUtils.geneMatrix(placeHolder, assembleHint, embeddings)
    matrix = matStats._1
    assembleStats = matStats._2
  }

  protected override def doPush(grad: Matrix, alpha: Double): Unit = {
    val map: JMap[JLong, Vector] = grad.asInstanceOf[MapMatrix[Vector]].getMap
    val normal = 1.0 / (placeHolder.getBatchSize * taskNum)

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

    val param = new UpdateColsParam(matrixId, (start until end).toArray, placeHolder.getIndices, map)
    val func = new UpdateColsFunc(param)
    PSAgentContext.get().getUserRequestAdapter.update(func).get()
  }
}
