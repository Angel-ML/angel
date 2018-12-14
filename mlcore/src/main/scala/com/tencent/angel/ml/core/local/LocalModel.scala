package com.tencent.angel.ml.core.local

import com.tencent.angel.ml.core.{Model, PredictResult}
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.data.{DataBlock, LabeledData}
import com.tencent.angel.ml.core.network.{EvnContext, Graph}
import com.tencent.angel.ml.core.network.layers.PlaceHolder
import com.tencent.angel.ml.core.utils.JsonUtils

class LocalModel(conf: SharedConf) extends Model {
  implicit val graph: Graph = new LocalGraph(new PlaceHolder(conf), conf)

  override def buildNetwork(): Unit = {
    JsonUtils.layerFromJson(conf.getJson)
  }

  override def init(taskFlag: Int): Unit = {
    graph.init(taskFlag)
  }

  override def createMatrices(envCtx: EvnContext): Unit = {
    graph.createMatrices()
  }

  override def loadModel(envCtx: EvnContext, path: String): Unit = {
    graph.loadModel(envCtx, path)
  }

  override def saveModel(envCtx: EvnContext, path: String): Unit = {
    graph.saveModel(envCtx, path)
  }

  override def predict(storage: DataBlock[LabeledData]): DataBlock[PredictResult] = {
    graph.predict()
    null
  }

  override def predict(storage: LabeledData): PredictResult = {
    graph.predict()
    null
  }
}
