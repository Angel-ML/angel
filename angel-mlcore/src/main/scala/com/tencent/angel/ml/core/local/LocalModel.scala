package com.tencent.angel.ml.core.local

import com.tencent.angel.ml.core.{GraphModel, PredictResult}
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.data.DataBlock
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.layers.PlaceHolder
import com.tencent.angel.ml.core.utils.JsonUtils
import com.tencent.angel.ml.core.variable.{VariableManager, VariableProvider}
import com.tencent.angel.ml.math2.utils.LabeledData


class LocalModel(conf: SharedConf) extends GraphModel {
  override protected val placeHolder: PlaceHolder = new PlaceHolder(conf)
  override protected implicit val variableManager: VariableManager = LocalVariableManager.get(isSparseFormat)
  override protected val variableProvider: VariableProvider = new LocalVariableProvider(dataFormat, modelType, placeHolder)

  override implicit val graph: Graph = new Graph(variableProvider, placeHolder, conf, 1)

  override def buildNetwork(): Unit = {
    JsonUtils.layerFromJson(conf.getJson)
  }

  def predict(storage: DataBlock[LabeledData]): List[PredictResult] = {
    graph.feedData((0 until storage.size()).toArray.map { idx => storage.get(idx) })
    pullParams(-1)
    graph.predict()
  }

  def predict(storage: LabeledData): PredictResult = {
    graph.feedData(Array(storage))
    pullParams(-1)
    graph.predict().head
  }
}
