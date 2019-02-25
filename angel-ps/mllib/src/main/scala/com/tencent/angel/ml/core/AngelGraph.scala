package com.tencent.angel.ml.core

import com.tencent.angel.client.AngelClient
import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.network.layers.PlaceHolder
import com.tencent.angel.ml.core.network.{EvnContext, Graph}
import com.tencent.angel.ml.core.variable.VariableManager
import com.tencent.angel.ml.math2.utils.RowType


case class AngelEvnContext(angelClient: AngelClient) extends EvnContext

class AngelGraph(placeHolder: PlaceHolder, conf: SharedConf, override val taskNum: Int)
  extends Graph(placeHolder, SharedConf.variableProvider())
    with Serializable {
  override val indexRange: Long = SharedConf.indexRange
  override val validIndexNum: Long = SharedConf.modelSize

  override val dataFormat: String = SharedConf.inputDataFormat
  override val modelType: RowType = SharedConf.modelType
  private val isSparseFormat = dataFormat == "libsvm" || dataFormat == "dummy"
  override val variableManager: VariableManager = new VariableManager(isSparseFormat)

  override def normalFactor: Double = 1.0 / (placeHolder.getBatchSize * taskNum)

  override def toString: String = super.toString
}
