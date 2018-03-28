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

import com.tencent.angel.master.AngelApplicationMaster
import com.tencent.angel.master.ps.attempt.{PSAttemptEvent, PSAttemptEventType}
import com.tencent.angel.plugin.AngelService
import com.tencent.angel.serving.ServingHost
import com.tencent.angel.serving.master.ServingServiceImpl
import com.tencent.angel.serving.protocol.ServingService
import com.tencent.angel.serving.protocol.protocolPB.{AgentProtocolPB, AgentProtocolServerTranslatorPB, ClientProtocolPB, ClientProtocolServerTranslatorPB}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.event.EventHandler

/**
  * the plugin for serving master
  */
class ServingMaster extends AngelService[AngelApplicationMaster] {

  private[this] var _config: Configuration = _

  def config: Configuration = _config

  def config_=(value: Configuration): Unit = {
    _config = value
  }

  lazy val servingService: ServingService = {
    new ServingServiceImpl(config)
  }

  override def check(who: scala.Any): Boolean = {
    who.isInstanceOf[AngelApplicationMaster]
  }


  override def start(master: AngelApplicationMaster): Unit = {
    val rpcServer = master.getAppContext.getMasterService.getRpcServer
    val servingManager = servingService.asInstanceOf[ServingServiceImpl].servingServiceManager
    val psManager = master.getAppContext.getParameterServerManager
    rpcServer.addProtocolImpl(classOf[ClientProtocolPB], new ClientProtocolServerTranslatorPB(servingService))
    rpcServer.addProtocolImpl(classOf[AgentProtocolPB], new AgentProtocolServerTranslatorPB(servingService))

    val psEventHandler = new EventHandler[PSAttemptEvent] {
      override def handle(event: PSAttemptEvent): Unit = {
        val psAttemptId = event.getPSAttemptId
        val ps = psManager.getParameterServer(psAttemptId.getPsId)
        val psAttempt = ps.getPSAttempt(psAttemptId)
        if (psAttempt != null) {
          event.getType match {
            case PSAttemptEventType.PA_REGISTER =>
              servingManager.onAddServingAgent(new ServingHost(psAttempt.getLocation.getIp))
            case PSAttemptEventType.PA_KILL | PSAttemptEventType.PA_FAILMSG | PSAttemptEventType.PA_UNREGISTER =>
              servingManager.onRemoveServingAgent(new ServingHost(psAttempt.getLocation.getIp))
            case _ =>
          }
        }
      }
    }

    master.getAppContext.getDispatcher.register(classOf[PSAttemptEventType], psEventHandler)
    servingService.start()
  }


  override def stop(): Unit = {
    servingService.stop()
  }

  override def setConf(paramConfiguration: Configuration): Unit = {
    config = paramConfiguration
  }

  override def getConf: Configuration = config
}
