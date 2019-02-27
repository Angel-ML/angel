package com.tencent.angel.ml.core.local

import com.tencent.angel.ml.core.network.EvnContext
import com.tencent.angel.ml.core.variable.VariableManager
import com.tencent.angel.ml.math2.vector.Vector

import scala.collection.JavaConversions._

class LocalVariableManager private(isSparseFormat: Boolean) extends VariableManager {

  override def createALL(envCtx: EvnContext): Unit = {
    variables.values().foreach { variable => variable.create(envCtx) }
  }

  override def loadALL(envCtx: EvnContext, path: String): Unit = {
    variables.values().foreach { variable => variable.load(envCtx, path) }
  }

  override def pullALL(epoch: Int, indices: Vector = null): Unit = {
    // val isSparseFormat = graph.dataFormat == "libsvm" || graph.dataFormat == "dummy"

    variables.values().foreach {
      case variable if isSparseFormat && variable.allowPullWithIndex =>
        variable.pull(epoch, indices)
      case variable => variable.pull(epoch)
    }
  }

  override def pull(name: String, epoch: Int = 0, indices: Vector = null): Unit = {
    // val isSparseFormat = graph.dataFormat == "libsvm" || graph.dataFormat == "dummy"

    val variable = getVariable(name)
    if (variable != null) {
      variable match {
        case v if isSparseFormat && v.allowPullWithIndex =>
          v.pull(epoch, indices)
        case v => v.pull(epoch)
      }
    }

  }

  override def saveALL(envCtx: EvnContext, path: String): Unit = {
    variables.values().foreach { variable => variable.save(envCtx, path) }
  }
}

object LocalVariableManager {
  private var vm: VariableManager = _

  def get(isSparseFormat: Boolean): VariableManager = synchronized {
    if (vm == null) {
      vm = new LocalVariableManager(isSparseFormat)
    }
    vm
  }
}
