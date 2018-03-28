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

package com.tencent.angel.serving.plugin

import java.net.InetSocketAddress

import com.tencent.angel.plugin.AngelService
import com.tencent.angel.ps.impl.ParameterServer
import com.tencent.angel.serving.ServingHost
import com.tencent.angel.serving.agent.{AgentService, AgentServiceImpl, ServingAgentManager}
import com.tencent.angel.serving.protocol.protocolPB.AgentProtocolClientTranslatorPB
import org.apache.hadoop.conf.Configuration

/**
  * the plugin for serving agent
  */
class ServingAgent extends AngelService[ParameterServer] {

  private[this] var _config: Configuration = _

  def config: Configuration = _config

  def config_=(value: Configuration): Unit = {
    _config = value
  }

  var agentService: AgentService = _

  override def check(who: scala.Any): Boolean = {
    who.isInstanceOf[ParameterServer]
  }


  override def start(agent: ParameterServer): Unit = {
    val masterLoc = agent.getMasterLocation
    val servingAgentManager: ServingAgentManager = new ServingAgentManager(
      getConf,
      new ServingHost(agent.getHostAddress),
      new AgentProtocolClientTranslatorPB(new InetSocketAddress(masterLoc.getIp, masterLoc.getPort), getConf)
    )
    agentService = new AgentServiceImpl(getConf, servingAgentManager)
    agentService.start()
  }


  override def stop(): Unit = agentService.stop()


  override def setConf(paramConfiguration: Configuration): Unit = {
    config = paramConfiguration
  }

  override def getConf: Configuration = config
}
