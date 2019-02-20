package com.tencent.angel.ml.core

import com.tencent.angel.ml.core.data.DataBlock
import com.tencent.angel.ml.core.network.EvnContext
import com.tencent.angel.ml.core.variable.{Variable, VariableManager}
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.utils.LabeledData
import com.tencent.angel.ml.math2.vector.Vector


abstract class MLModel {
  val variableManager: VariableManager

  def addVariable(variable: Variable): Unit = {
    variableManager.addVariable(variable)
  }

  def getVariable(name: String): Variable = {
    variableManager.getVariable(name)
  }

  def getAllVariables: List[Variable] = {
    variableManager.getALLVariables
  }

  def hasVariable(v: Variable): Boolean = variableManager.hasVariable(v)

  def hasVariable(name: String): Boolean = variableManager.hasVariable(name)

  def putSlot(v: Variable, g: Matrix): Unit = {
    if (variableManager.hasSlot(v.name)) {
      variableManager.getSlot(v.name).iadd(g)
    } else {
      variableManager.putSlot(v, g)
    }
  }

  def getSlot(name: String): Matrix = {
    variableManager.getSlot(name)
  }

  def getAllSlots: Map[String, Matrix] = {
    variableManager.getAllSlots
  }

  def hasSlot(name: String): Boolean = variableManager.hasSlot(name)

  def putGradient(v: Variable, g: Matrix): Unit = putSlot(v, g)

  def getAllGradients: Map[String, Matrix] = getAllSlots

  def getGradient(name: String): Matrix = getSlot(name)

  def hasGradient(name: String): Boolean = hasSlot(name)

  //---------------------Training Cycle
  def createMatrices(envCtx: EvnContext): Unit = {
    variableManager.createALL(envCtx)
  }

  def init(taskId: Int = 0): Unit = {
    variableManager.initALL(taskId)
  }

  def pullParams(epoch: Int, indices: Vector = null): Unit = {
    variableManager.pullALL(epoch, indices)
  }

  def pushSlot(lr: Double): Unit = {
    variableManager.pushALL(lr)
  }

  def update[T](epoch: Int, batchSize: Int): Unit = {
    variableManager.updateALL[T](epoch, batchSize)
  }

  def loadModel(envCtx: EvnContext, path: String): Unit = {
    variableManager.loadALL(envCtx, path)
  }

  def saveModel(envCtx: EvnContext, path: String): Unit = {
    variableManager.saveALL(envCtx, path)
  }

  //---------------------Predict
  def predict(storage: DataBlock[LabeledData]): List[PredictResult]

  def predict(storage: LabeledData): PredictResult
}
