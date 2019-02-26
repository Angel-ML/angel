package com.tencent.angel.ml

import com.tencent.angel.client.AngelClient
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.core.AngelEvnContext
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.EvnContext
import com.tencent.angel.ml.core.variable.{PSVariable, VarState, VariableManager}
import com.tencent.angel.model.{MatrixSaveContext, ModelSaveContext}

class AngelVariableManager(isSparseFormat: Boolean) extends VariableManager(isSparseFormat) {

  override def createALL(envCtx: EvnContext): Unit = {
    envCtx match {
      case AngelEvnContext(client: AngelClient) if client != null =>
        getALLVariables.foreach {
          case variable: PSVariable => client.addMatrix(variable.getMatrixCtx)
          case _ =>
        }
        client.createMatrices()
        getALLVariables.foreach { variable => variable.setState(VarState.Created) }
      case _ =>
        getALLVariables.foreach { variable => variable.create(envCtx) }
    }
  }

  override def loadALL(envCtx: EvnContext, path: String): Unit = {
    envCtx match {
      case AngelEvnContext(client: AngelClient) if client != null =>
        client.load()
        getALLVariables.foreach { variable => variable.setState(VarState.Initialized) }
      case _ =>
        getALLVariables.foreach { variable => variable.load(envCtx, path) }
    }
  }

  override def saveALL(envCtx: EvnContext, path: String): Unit = {
    envCtx match {
      case AngelEvnContext(client: AngelClient) if client != null =>
        val sharedConf = SharedConf.get()
        val saveContext = new ModelSaveContext
        getALLVariables.foreach { variable =>
          assert(variable.getState == VarState.Initialized || variable.getState == VarState.Ready)
          saveContext.addMatrix(new MatrixSaveContext(variable.name, variable.formatClassName))
        }
        saveContext.setSavePath(sharedConf.get(AngelConf.ANGEL_JOB_OUTPUT_PATH))
        val deleteExistsFile = sharedConf.getBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST,
          AngelConf.DEFAULT_ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST)
        client.save(saveContext, deleteExistsFile)
      case _ =>
        getALLVariables.foreach { variable => variable.save(envCtx, path) }
    }
  }
}
