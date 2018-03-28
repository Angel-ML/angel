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

import java.util.concurrent.ConcurrentHashMap

import com.tencent.angel.serving.protocol.ClientProtocol
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

import scala.collection._
import scala.collection.convert.decorateAsScala._
import scala.util.control.NonFatal

/**
  * the client manager for serving
  *
  * @param clientService
  * @param config
  */
class ClientServingManager(clientService: ClientProtocol, config: Configuration) {

  val LOG = LogFactory.getLog(classOf[ClientServingManager])

  val monitorThread: Thread = {
    val thread = new Thread(new LocationMonitor(config.getLong("angel.serving.client.location.check.interval", 3 * 1000)))
    thread.setName("Distributed Model Location Monitor")
    thread.setDaemon(true)
    thread.start()
    thread
  }


  private[this] val distributedModelMap: concurrent.Map[String, DistributedModel] = new ConcurrentHashMap[String, DistributedModel]().asScala

  /**
    * register model, will ask master notify agent to load sharding model for serving
    *
    * @param model
    */
  def register(model: DistributedModel): Unit = {
    val present = distributedModelMap.putIfAbsent(model.name, model)
    require(present.isEmpty, s"${model.name} is present")
    if (LOG.isDebugEnabled) {
      LOG.debug(s"the model:${model.name} is registered")
    }
  }

  /**
    * unregister model and stop serving
    *
    * @param name
    */
  def unregister(name: String): Unit = {
    if (distributedModelMap.contains(name)) {
      distributedModelMap.remove(name)
      if (LOG.isDebugEnabled) LOG.debug(s"the model:${name} is unregistered")
    } else {
      if (LOG.isDebugEnabled)LOG.debug(s"the model:${name} is not exsits in distributedModelMap !")
    }
  }

  /**
    * is registered
    *
    * @param name the model name
    * @return true if registered else or not
    */
  def isRegistered(name: String): Boolean = distributedModelMap.contains(name)

  /**
    * is servable, each sharding model has at least 1 replica
    *
    * @param name the model name
    * @return true if servable else or not
    */
  def isServable(name: String): Boolean = {
    distributedModelMap.get(name).map(model => model.isServable()).getOrElse(false)
  }

  /**
    * get model
    *
    * @param name the model name
    * @return the model of option
    */
  def getModel(name: String): Option[DistributedModel] = {
    distributedModelMap.get(name)
  }

  /**
    * the location monitor which get model's location from master periodically
    *
    * @param interval
    */
  class LocationMonitor(interval: Long) extends Runnable {
    var stop = false

    override def run() = {
      while (!stop) {
        try {
          val modelLocationList = clientService.getModelLocations().modelLocations
          val modelLocs = modelLocationList.map(_.name).toSet

          modelLocationList.foreach{ modelLocation =>
            distributedModelMap.get(modelLocation.name).foreach{clientModel =>

              val splitLocs = modelLocation.splitLocations.map(splitLoc => (splitLoc.idx, splitLoc.locs)).toMap
              clientModel.splits.filter(modelSplit => splitLocs.contains(modelSplit.index)).foreach(modelSplit => modelSplit.replica.renew(splitLocs(modelSplit.index)))
            }
          }

          distributedModelMap.foreach((model: (String, DistributedModel)) => {
            if (!modelLocs.contains(model._1)) {
              model._2.splits.foreach(split => split.replica.cleanLoc)
            }
          })
          if (LOG.isDebugEnabled) {
            val localLocs = distributedModelMap.values.map(model => (model.name + "=" + model.splits.map(split => split.index + ":" + split.replica).mkString(","))).mkString(";")
            LOG.debug(s"remote locations:${modelLocationList.mkString(";")}, and local:$localLocs")
          }
          Thread.sleep(interval)
        } catch {
          case e: InterruptedException => {
            LOG.warn("location monitor interrupted,will quit", e)
            stop = true
          }
          case NonFatal(e) => LOG.error("location monitor occur error", e)
        }
      }
    }
  }

}



