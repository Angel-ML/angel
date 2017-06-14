package com.tencent.angel.ml.model

import java.util.{HashMap, Map}

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.worker.predict.PredictResult
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

/**
  * Model for a Algorithm. One MLModel is composed by one or multi PSModel, each one can be referred directly with Model name
  *
  * MLModel can be load from Path and save to Path, used to do prediction.
  *
  * @param _ctx
  * @param psModels
  */

abstract class MLModel(_ctx : TaskContext, psModels: Map[String, PSModel[_]] = new HashMap[String, PSModel[_]] ) {
  implicit def ctx : TaskContext = _ctx

  /**
    * Get all PSModels
    * @return a name to PSModel map
    */
  def getPSModels: Map[String, PSModel[_]] = {
    return psModels
  }

  /**
    * Get a PSModel use name. With this method, user can refer to one PSModel simply with mlModel.psModelName
    *
    * @param name PSModel name
    * @return
    */
  def getPSModel(name: String): PSModel[_] = {
    return psModels.get(name)
  }

  /**
    * Add a new PSModel
    * @param name PSModel name
    * @param psModel PSModel
    */
  def addPSModel(name: String, psModel: PSModel[_]) {
    psModels.put(name, psModel)
  }

  /**
    * Add a new PSModel
    * @param psModel PSModel
    */
  def addPSModel(psModel: PSModel[_]) {
    psModels.put(psModel.modelName, psModel)
  }

  /**
    * Predict use the PSModels and predict data
    * @param storage predict data
    * @return predict result
    */
  def predict(storage: DataBlock[LabeledData]): DataBlock[PredictResult]

  /**
    * Set save path for the PSModels
    * @param conf Application configuration
    */
  def setSavePath(conf: Configuration)

  /**
    * Set PSModels load path
    * @param conf Application configuration
    */
  def setLoadPath(conf: Configuration)

}
