package com.tencent.angel.ml.core

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.data.{DataBlock, LabeledData}
import com.tencent.angel.ml.core.network.{EvnContext, Graph}

abstract class Model {
  val graph: Graph

  def buildNetwork(): Unit

  def init(taskFlag: Int): Unit = {
    graph.init(taskFlag)
  }

  def createMatrices(envCtx: EvnContext): Unit = {
    graph.createMatrices(envCtx)
  }

  def loadModel(envCtx: EvnContext, path: String): Unit = {
    graph.loadModel(envCtx, path)
  }

  def saveModel(envCtx: EvnContext, path: String): Unit = {
    graph.saveModel(envCtx, path)
  }

  def predict(storage: DataBlock[LabeledData]): List[PredictResult] = {
    graph.feedData((0 until storage.size()).toArray.map {idx => storage.get(idx) })
    graph.pullParams(-1)
    graph.predict()
  }

  def predict(storage: LabeledData): PredictResult = {
    graph.feedData(Array(storage))
    graph.pullParams(-1)
    graph.predict().head
  }
}

object Model {

  def apply(className: String, conf: SharedConf): Model = {
    val cls = Class.forName(className)
    val cstr = cls.getConstructor(classOf[SharedConf])
    cstr.newInstance(conf).asInstanceOf[Model]
  }
}
