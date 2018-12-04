package com.tencent.angel.ml.core.network.variable

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.graph.Graph
import com.tencent.angel.ml.core.network.variable.Variable.Location
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}

abstract class PSVariable(name: String, rowType: RowType)(implicit graph: Graph)
  extends Variable(name, rowType, Location.PS)(graph) {
  protected val rowsSaved: Array[Int]
  protected val ctx: MatrixContext
  protected lazy val normal: Double = 1.0 / graph.getNormal

  def getMatrixCtx: MatrixContext = ctx

  def loadParams(loadContext: ModelLoadContext): Unit = {
    loadContext.addMatrix(new MatrixLoadContext(name))
  }

  def saveParams(saveContext: ModelSaveContext): Unit = {
    val outputFormat = SharedConf.sparseInputLayerMatrixOutputFormat
    val msc: MatrixSaveContext = new MatrixSaveContext(name, outputFormat)
    msc.addIndices(rowsSaved)
    saveContext.addMatrix(msc)
  }
}
