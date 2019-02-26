package com.tencent.angel.ml.core

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.data.DataBlock
import com.tencent.angel.ml.core.network.{EvnContext, Graph}
import com.tencent.angel.ml.core.utils.MethodNotImplement
import com.tencent.angel.ml.core.variable.{Variable, VariableManager}
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.utils.LabeledData

abstract class GraphModel extends MLModel {
  val graph: Graph
  override val variableManager: VariableManager = graph.variableManager

  def buildNetwork(): Unit

  override def addVariable(variable: Variable): Unit = {
    throw MethodNotImplement("addVariable is not implement in GraphModel")
  }

  override def putSlot(v: Variable, g: Matrix): Unit = {
    throw MethodNotImplement("addVariable is not implement in GraphModel")
  }

  def pushGradient(lr: Double): Unit = {
    pushSlot(lr)
  }

  def predict(storage: DataBlock[LabeledData]): List[PredictResult] = {
    graph.feedData((0 until storage.size()).toArray.map {idx => storage.get(idx) })
    pullParams(-1)
    graph.predict()
  }

  def predict(storage: LabeledData): PredictResult = {
    graph.feedData(Array(storage))
    pullParams(-1)
    graph.predict().head
  }
}

object GraphModel {

  def apply(className: String, conf: SharedConf): GraphModel = {
    val cls = Class.forName(className)
    val cstr = cls.getConstructor(classOf[SharedConf])
    cstr.newInstance(conf).asInstanceOf[GraphModel]
  }
}
