package com.tencent.angel.ml.core.variable

import java.util.concurrent

import com.tencent.angel.ml.core.network.EnvContext
import com.tencent.angel.ml.core.variable.VarState.VarState
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.vector._

import scala.collection.JavaConversions._
import scala.reflect.runtime.{universe => ru}

abstract class VariableManager {
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
    variables.values().foreach { variable => variable.setState(state) }
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

  def createALL[T](envCtx: EnvContext[T]): Unit

  def create[T](envCtx: EnvContext[T], name: String): Unit = {
    val variable = getVariable(name)

    if (variable != null) {
      variable.create(envCtx)
    }
  }

  def initALL[T](envCtx: EnvContext[T], taskId: Int = 0, mean: Double = 0.0, stddev: Double = 0.00001): Unit = {
    variables.values().foreach { variable => variable.init(taskId, mean, stddev) }
  }

  def init[T](envCtx: EnvContext[T], name: String, mean: Double = 0.0, stddev: Double = 0.00001): Unit = {
    val variable = getVariable(name)

    if (variable != null) {
      variable.init(0, mean, stddev)
    }
  }

  def pullALL(epoch: Int, indices: Vector = null): Unit

  def pull(name: String, epoch: Int = 0, indices: Vector = null): Unit

  def pushALL(alpha: Double = 1.0): Unit = {
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

  def push(name: String, alpha: Double = 1.0): Unit = {
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

  def loadALL[T](envCtx: EnvContext[T], path: String): Unit

  def load[T](name: String, envCtx: EnvContext[T], path: String): Unit = {
    val variable = getVariable(name)

    if (variable != null) {
      variable.load(envCtx, path)
    }
  }

  def saveALL[T](envCtx: EnvContext[T], path: String): Unit

  def save[T](name: String, envCtx: EnvContext[T], path: String): Unit = {
    val variable = getVariable(name)

    if (variable != null) {
      variable.save(envCtx, path)
    }
  }
}


object VariableManager {
  private var vm: VariableManager = _

  def get(name: String, isSparseFormat: Boolean): VariableManager = synchronized {
    if (vm == null) {
      val rtMirror = ru.runtimeMirror(getClass.getClassLoader)
      val objModuleSymbol = rtMirror.staticModule(name)
      val objModuleMirror = rtMirror.reflectModule(objModuleSymbol)
      val method = objModuleMirror.symbol.typeSignature.member(ru.TermName("get")).asMethod
      val objMirror = rtMirror.reflect(objModuleMirror.instance)
      val result = objMirror.reflectMethod(method)(isSparseFormat)
      vm = result.asInstanceOf[VariableManager]
    }

    vm
  }

  def addVariable(v: Variable): Unit = {
    assert(vm != null)
    vm.addVariable(v)
  }
}