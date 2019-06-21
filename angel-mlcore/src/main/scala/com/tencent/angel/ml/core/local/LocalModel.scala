package com.tencent.angel.ml.core.local

import com.tencent.angel.ml.core.{GraphModel, PredictResult}
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.data.DataBlock
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.utils.JsonUtils
import com.tencent.angel.ml.core.variable.{VariableManager, VariableProvider}
import com.tencent.angel.ml.servingmath2.utils.LabeledData


class LocalModel(conf: SharedConf) extends GraphModel(conf) {
  private implicit val sharedConf: SharedConf = conf

  override protected implicit val variableManager: VariableManager = new LocalVariableManager(isSparseFormat, conf)
  override protected val variableProvider: VariableProvider = new LocalVariableProvider(dataFormat, modelType)

  override implicit val graph: Graph = new Graph(variableProvider, conf, 1)

  override def buildNetwork(): this.type = {
    JsonUtils.layerFromJson(conf.getJson)

    this
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
