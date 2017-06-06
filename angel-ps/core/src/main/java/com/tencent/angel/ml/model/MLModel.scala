package com.tencent.angel.ml.model

import java.util.{HashMap, Map}

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.worker.predict.PredictResult
import com.tencent.angel.worker.storage.DataBlock
import org.apache.hadoop.conf.Configuration


abstract class MLModel {
  private var psModels: Map[String, PSModel[_]] = new HashMap[String, PSModel[_]]

  def this(psModels: Map[String, PSModel[_]]) {
    this()
    this.psModels = psModels
  }

  def getPsModels: Map[String, PSModel[_]] = {
    return psModels
  }

  def addPSModel(name: String, psModel: PSModel[_]) {
    psModels.put(name, psModel)
  }

  def addPSModel(psModel: PSModel[_]) {
    psModels.put(psModel.getName, psModel)
  }

  def predict(storage: DataBlock[LabeledData]): DataBlock[PredictResult]

  def setSavePath(conf: Configuration)

  def setLoadPath(conf: Configuration)

}
