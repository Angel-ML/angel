/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 *  Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *  https://opensource.org/licenses/BSD-3-Clause
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the License
 *  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 *  or implied. See the License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

package com.tencent.angel.serving.master


import com.tencent.angel.serving.ServingLocation
import com.tencent.angel.serving.common.{ModelCommand, ModelDefinition, ModelLocationList, ModelReport}
import com.tencent.angel.serving.protocol.ServingService
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

/**
  *the implement of serving service
  * @param config
  */
class ServingServiceImpl(config: Configuration) extends ServingService {
  val LOG = LogFactory.getLog(classOf[ServingServiceImpl])

  val servingServiceManager: ServingServiceManager = new ServingServiceManager(config)

  override def start(): Unit = servingServiceManager.start()

  override def stop(): Unit = servingServiceManager.exit()

  override def registerAgent(agent: ServingLocation): Unit = {
    servingServiceManager.registerServingAgent(agent)
  }

  override def registerModel(model: ModelDefinition, shardingModelClass:String): Boolean = {
    LOG.info(s"registerModel: \t$shardingModelClass")
    servingServiceManager.registerModel(model, shardingModelClass)
  }

  override def unregisterModel(name: String): Boolean = {
    servingServiceManager.unregisterModel(name)
  }

  override def modelReport(modelReport: ModelReport): ModelCommand = {
    servingServiceManager.modelReport(modelReport)
  }

  override def getModelLocations(): ModelLocationList = {
    servingServiceManager.getModelLocations()
  }
}
