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

package com.tencent.angel.serving.client

import java.net.InetSocketAddress

import com.tencent.angel.client.{AngelClient, AngelContext}
import com.tencent.angel.serving.protocol.protocolPB.ClientProtocolClientTranslatorPB
import com.tencent.angel.serving.transport.serving.ServingTransportClient
import org.apache.hadoop.conf.Configuration

/**
  * the client of serving
  */
trait ServingClient {

  /**
    * ask to load the model for serving if the model is linear which can split as sharding model
    *
    * @param name       the model name
    * @param dir        the model dir
    * @param replica    the num of sharding serving nodes
    * @param concurrent the concurrent capacity for serving
    */
  def loadModel(name: String, dir: String, replica: Int, concurrent: Int, shardingModelClass: String): Boolean

  /**
    * ask to load the model for serving, use specified splitter and coordinator
    *
    * @param name         the model name
    * @param dir          the model dir
    * @param servingNodes the num of sharding serving nodes
    * @param concurrent   the concurrent capacity for serving
    * @param splitter     the splitter for model and predict data
    * @param coordinator  the coordinator for serving
    */
  def loadModel(name: String, dir: String, servingNodes: Int, concurrent: Int,
                splitter: ModelSplitter, coordinator: ModelCoordinator, shardingModelClass: String): Boolean

  def loadModelLocal(name: String, dir: String, replica: Int, concurrent: Int)

  /**
    * get the model by name
    *
    * @param name the model name
    * @return the model of option
    */
  def getModel(name: String): Option[DistributedModel]

  /**
    * model is servable
    *
    * @param name the model name
    * @return true if servable else or not
    */
  def isServable(name: String): Boolean

  /**
    * ask to unload model
    *
    * @param name
    */
  def unloadModel(name: String): Boolean

  /**
    * get the model router while model is loaded
    *
    * @param name
    * @return
    */
  def getRouter(name: String): ModelRouter
}

/**
  *
  */
object ServingClient {

  def create(server: InetSocketAddress, conf: Configuration): ServingClient = {
    val transportClient = ServingTransportClient.create(conf)
    val protocolProxy = new ClientProtocolClientTranslatorPB(server, conf)
    new ServingClientImpl(protocolProxy, transportClient, conf)
  }

  def create(angelClient: AngelClient): ServingClient = {
    val conf = angelClient.getConf
    val masterLoc = angelClient.getMasterLocation
    val masterAddr = new InetSocketAddress(masterLoc.getIp, masterLoc.getPort)
    create(masterAddr, conf)
  }

  def create(angelContext: AngelContext): ServingClient = {
    val conf = angelContext.getConf
    val masterAddr = new InetSocketAddress(angelContext.getMasterLocation.getIp, angelContext.getMasterLocation.getPort)
    create(masterAddr, conf)
  }


}


