package com.tencent.angel.ml.core.variable

import java.util.concurrent

import com.tencent.angel.ml.core.network.{EvnContext, Graph}
import com.tencent.angel.ml.core.variable.VarState.VarState
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.math2.matrix.Matrix

import scala.collection.JavaConversions._


class VariableManager(isSparseFormat: Boolean) {
  protected val variables = new concurrent.ConcurrentHashMap[String, Variable]()
  protected val slots = new concurrent.ConcurrentHashMap[String, Matrix]()

  def addVariable(v: Variable): Unit = {
    variables.put(v.name, v)
  }

  def getALLVariables: List[Variable] = variables.values().toList

  def getVariable(name: String): Variable = {
    variables.getOrDefault(name, null.asInstanceOf[Variable])
  }

  def hasVariable(v: Variable): Boolean = {
    variables.contains(v.name)
  }

  def hasVariable(varName: String): Boolean = {
    variables.contains(varName)
  }

  def putSlot(v: Variable, g: Matrix): Unit = {
    slots.put(v.name, g)
  }

  def getAllSlots: Map[String, Matrix] = {
    slots.map { case (name: String, grad: Matrix) =>
      name -> grad
    }.toMap
  }

  def getSlot(name: String): Matrix = {
    slots.getOrDefault(name, null.asInstanceOf[Matrix])
  }

  def hasSlot(gradName: String): Boolean = {
    slots.contains(gradName)
  }

  def setAllState(state: VarState): Unit = {
    variables.values().foreach{ variable => variable.setState(state)}
  }

  def setState(name: String, state: VarState): Unit = {
    val variable = getVariable(name)

    if (variable != null) {
      variable.setState(state)
    }
  }

  /** **********************************************************************************
    * Variable operation
    */

  def createALL(envCtx: EvnContext): Unit = {
    variables.values().foreach { variable => variable.create(envCtx) }
  }

  def create(envCtx: EvnContext, name: String): Unit = {
    val variable = getVariable(name)

    if (variable != null) {
      variable.create(envCtx)
    }
  }

  def initALL(taskId: Int = 0, mean: Double = 0.0, stddev: Double = 0.00001): Unit = {
    variables.values().foreach { variable => variable.init(taskId, mean, stddev) }
  }

  def init(name: String, mean: Double = 0.0, stddev: Double = 0.00001): Unit = {
    val variable = getVariable(name)

    if (variable != null) {
      variable.init(0, mean, stddev)
    }
  }

  def pullALL(epoch: Int, indices: Vector = null): Unit = {
    // val isSparseFormat = graph.dataFormat == "libsvm" || graph.dataFormat == "dummy"

    variables.values().foreach {
      case variable if isSparseFormat && variable.allowPullWithIndex =>
        variable.pull(epoch, indices)
      case variable => variable.pull(epoch)
    }
  }

  def pull(name: String, epoch: Int = 0, indices: Vector = null): Unit = {
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

  def pushALL(alpha: Double=1.0): Unit = {
    variables.values().foreach {
      case matVar: MatVariable =>
        val matSlot = getSlot(matVar.name)
        if (matSlot != null) {
          matVar.push(matSlot, alpha)
        }
      case vecVar: VecVariable =>
        val vecSlot = getSlot(vecVar.name)
        if (vecSlot != null) {
          vecVar.push(vecSlot, alpha)
        }
    }
  }

  def push(name: String, alpha: Double=1.0): Unit = {
    val variable = getVariable(name)
    val grad = getSlot(name)

    if (variable != null && grad != null) {
      variable.push(grad, alpha)
    }
  }

  def updateALL[T](epoch: Int, batchSize: Int): Unit = {
    val futures = variables.values().map { variable =>
      variable.update[T](epoch, batchSize)
    }

    futures.foreach {
      case fu if fu != null => fu.get()
      case _ =>
    }
  }

  def update[T](name: String, epoch: Int, batchSize: Int): Unit = {
    val variable = getVariable(name)

    if (variable != null) {
      val future = variable.update[T](epoch, batchSize)

      if (future != null) {
        future.get()
      }
    }
  }

  def loadALL(envCtx: EvnContext, path: String): Unit = {
    variables.values().foreach { variable => variable.load(envCtx, path) }
  }

  def load(name: String, envCtx: EvnContext, path: String): Unit = {
    val variable = getVariable(name)

    if (variable != null) {
      variable.load(envCtx, path)
    }
  }

  def saveALL(envCtx: EvnContext, path: String): Unit = {
    variables.values().foreach { variable => variable.save(envCtx, path) }
  }

  def save(name: String, envCtx: EvnContext, path: String): Unit = {
    val variable = getVariable(name)

    if (variable != null) {
      variable.save(envCtx, path)
    }
  }
}
