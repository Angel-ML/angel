package com.tencent.angel.ml.core

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.data.{DataBlock, LabeledData}
import com.tencent.angel.ml.core.network.{EvnContext}

trait Model {
  def buildNetwork(): Unit

  def init(taskFlag: Int): Unit

  def createMatrices(envCtx: EvnContext): Unit

  def loadModel(envCtx: EvnContext, path: String): Unit

  def saveModel(envCtx: EvnContext, path: String): Unit

  def predict(storage: DataBlock[LabeledData]): DataBlock[PredictResult]

  def predict(storage: LabeledData): PredictResult
}

object Model {

  def apply(className: String, conf: SharedConf): Model = {
    val cls = Class.forName(className)
    val cstr = cls.getConstructor(classOf[SharedConf])
    cstr.newInstance(conf).asInstanceOf[Model]
  }
}
