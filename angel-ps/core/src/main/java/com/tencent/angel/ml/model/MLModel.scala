package com.tencent.angel.ml.model

import java.util.{HashMap, Map}

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.worker.predict.PredictResult
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration


abstract class MLModel(_ctx : TaskContext, psModels: Map[String, PSModel[_]] = new HashMap[String, PSModel[_]] ) {
  implicit def ctx : TaskContext = _ctx

  def getPsModels: Map[String, PSModel[_]] = {
    return psModels
  }

  def getPsModel(name: String): PSModel[_] = {
    return psModels.get(name)
  }

  def addPSModel(name: String, psModel: PSModel[_]) {
    psModels.put(name, psModel)
  }

  def addPSModel(psModel: PSModel[_]) {
    psModels.put(psModel.modelName, psModel)
  }

  def predict(storage: DataBlock[LabeledData]): DataBlock[PredictResult]

  def setSavePath(conf: Configuration)

  def setLoadPath(conf: Configuration)

}
