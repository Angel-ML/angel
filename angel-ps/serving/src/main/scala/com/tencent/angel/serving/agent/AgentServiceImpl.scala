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

package com.tencent.angel.serving.agent

import com.tencent.angel.exception.AngelException
import com.tencent.angel.serving.common.ModelSplitID
import com.tencent.angel.serving.transport.serving.ServingRequestMessageHandler
import com.tencent.angel.serving.{PredictResult, PredictResultList, ShardingData}
import com.tencent.angel.transport.{NetworkContext, NetworkServer}
import org.apache.hadoop.conf.Configuration

/**
  * the implement of AgentService,offer predict function by netty transfer and communicate with ServingService by rpc
  *
  * @param conf                the configuration
  * @param servingAgentManager the manager for serving agent
  */
class AgentServiceImpl(val conf: Configuration, servingAgentManager: ServingAgentManager) extends AgentService {

  var server: NetworkServer = _


  def servable(): Boolean = {
    server != null
  }

  override def start(): Unit = afterAgentStart

  override def stop(): Unit = onAgentClose

  def afterAgentStart(): Unit = {
    server = new NetworkContext(conf, new ServingRequestMessageHandler(this, servingAgentManager))
      .createServer(servingAgentManager.servingHost.ip)
    servingAgentManager.register(server.getPort)
  }

  def onAgentClose(): Unit = {
    if (server != null) {
      server.close()
    }
    servingAgentManager.exit()
  }

  override def predict(modelSplitID: ModelSplitID, shardingData: ShardingData): PredictResult = {
    servingAgentManager.getModel(modelSplitID).getOrElse({
      throw new AngelException(s"$modelSplitID specific model not found")
    }).predict(shardingData)
  }

  override def batchPredict(modelSplitID: ModelSplitID, shardingData: Array[ShardingData]): PredictResultList = {
    val model = servingAgentManager.getModel(modelSplitID).getOrElse({
      throw new AngelException(s"$modelSplitID specific model not found")
    })
    val results = shardingData.map(data => model.predict(data).score)
    new PredictResultList(results)
  }
}
