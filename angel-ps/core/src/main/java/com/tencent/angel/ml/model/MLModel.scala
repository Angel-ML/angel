/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.ml.model

import java.util.{HashMap, Map}

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.predict.PredictResult
import com.tencent.angel.worker.storage.DataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConversions._

/**
  * Model for a Algorithm. One MLModel is composed by one or multi PSModel, each one can be referred directly with Model name
  *
  * MLModel can be load from Path and save to Path, used to do prediction.
  *
  * @param _ctx
  */

abstract class MLModel(conf: Configuration, _ctx: TaskContext = null) {

  implicit def ctx: TaskContext = _ctx

  private val psModels: Map[String, PSModel] = new HashMap[String, PSModel]

  /**
    * Get all PSModels
    *
    * @return a name to PSModel map
    */
  def getPSModels: Map[String, PSModel] = {
    return psModels
  }

  /**
    * Get a PSModel use name. With this method, user can refer to one PSModel simply with mlModel.psModelName
    *
    * @param name PSModel name
    * @return
    */
  def getPSModel(name: String): PSModel = {
    return psModels.get(name)
  }

  /**
    * Add a new PSModel
    *
    * @param name    PSModel name
    * @param psModel PSModel
    */
  def addPSModel(name: String, psModel: PSModel): this.type = {
    psModels.put(name, psModel)
    this
  }

  /**
    * Add a new PSModel
    *
    * @param psModel PSModel
    */
  def addPSModel(psModel: PSModel): this.type = {
    psModels.put(psModel.modelName, psModel)
    this
  }

  /**
    * Predict use the PSModels and predict data
    *
    * @param storage predict data
    * @return predict result
    */
  def predict(storage: DataBlock[LabeledData]): DataBlock[PredictResult]

  /**
    * Set save path for the PSModels
    *
    * @param conf Application configuration
    */
  def setSavePath(conf: Configuration): this.type = {
    val path = conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH)
    if (path != null)
      psModels.values().foreach { case model: PSModel =>
        if (model.needSave) model.setSavePath(path)
      }
    this
  }

  /**
    * Set PSModels load path
    *
    * @param conf Application configuration
    */
  def setLoadPath(conf: Configuration): this.type = {
    val path = conf.get(AngelConf.ANGEL_LOAD_MODEL_PATH)
    if (path != null)
      psModels.values().foreach { case model: PSModel =>
        if (model.needSave) model.setLoadPath(path)
      }
    this
  }

}
