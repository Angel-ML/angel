package com.tencent.angel.ml.core.variable

import java.util

import com.tencent.angel.client.AngelClient
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.ml.core.AngelEnvContext
import com.tencent.angel.mlcore.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.mlcore.network.EnvContext
import com.tencent.angel.model._

class AngelCILSImpl(val conf: SharedConf) extends CILSImpl {

  def doCreate[T](mCtx: MatrixContext, envCtx: EnvContext[T]): Unit = {
    envCtx match {
      case AngelEnvContext(client: AngelClient) if client != null =>
        val mcList = new util.ArrayList[MatrixContext]()
        mcList.add(mCtx)
        client.createMatrices(mcList)
      case _ =>
    }
  }

  override def doInit[T](mCtx: MatrixContext, envCtx: EnvContext[T], taskFlag: Int): Unit = ???

  override def doLoad[T](mCtx: MatrixContext, envCtx: EnvContext[T], path: String): Unit = {
    envCtx match {
      case AngelEnvContext(client: AngelClient) if client != null =>
        val loadContext = new ModelLoadContext(path)
        loadContext.addMatrix(new MatrixLoadContext(mCtx.getName))
        client.load(loadContext)
      case _ =>
    }
  }

  override def doSave[T](mCtx: MatrixContext, indices: Array[Int],
                         envCtx: EnvContext[T], path: String): Unit = {
    envCtx match {
      case AngelEnvContext(client: AngelClient) if client != null =>
        val saveContext: ModelSaveContext = new ModelSaveContext(path)
        val msc: MatrixSaveContext = new MatrixSaveContext(mCtx.getName,
          mCtx.getAttributes.get(MLCoreConf.ML_MATRIX_OUTPUT_FORMAT))
        msc.addIndices(indices)
        saveContext.addMatrix(msc)

        if (PSVariable.isFirstSave.getAndSet(false)) {
          val deleteExistsFile = conf.getBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST,
            AngelConf.DEFAULT_ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST)
          client.save(saveContext, deleteExistsFile)
        } else {
          client.save(saveContext, false)
        }
      case _ =>
    }
  }
}
