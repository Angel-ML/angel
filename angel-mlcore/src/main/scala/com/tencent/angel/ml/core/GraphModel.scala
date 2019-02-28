package com.tencent.angel.ml.core

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.optimizer.loss.LossFunc
import com.tencent.angel.ml.core.utils.MethodNotImplement
import com.tencent.angel.ml.core.variable.Variable
import com.tencent.angel.ml.math2.matrix.Matrix


abstract class GraphModel extends MLModel {
  val graph: Graph

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

  def lossFunc: LossFunc = graph.getLossFunc
}

object GraphModel {

  def apply(className: String, conf: SharedConf): GraphModel = {
    val cls = Class.forName(className)
    val cstr = cls.getConstructor(classOf[SharedConf])
    cstr.newInstance(conf).asInstanceOf[GraphModel]
  }
}
