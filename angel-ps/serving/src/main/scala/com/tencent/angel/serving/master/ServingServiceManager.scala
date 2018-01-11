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

import java.util
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import com.tencent.angel.serving.common.{ModelCommand, _}
import com.tencent.angel.serving.{ServingHost, ServingLocation}
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.Daemon

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{concurrent, mutable}
import scala.util.Random
import scala.util.control.NonFatal

/**
  * the manager of serving service, receive client's request and command the serving agent
  *
  * @param conf the configuration
  */
class ServingServiceManager(conf: Configuration) {

  val LOG = LogFactory.getLog(classOf[ServingServiceManager])

  lazy val monitor = new Daemon(new ServingModelMonitor(conf.getLong("angel.serving.model.monitor.interval", 3 * 1000)))

  val servingAgentHosts: mutable.Set[ServingHost] = Collections.synchronizedSet(new util.HashSet[ServingHost]()).asScala

  val servingAgentLocs: mutable.Set[ServingLocation] = Collections.synchronizedSet(new util.HashSet[ServingLocation]()).asScala

  val servingAgentMap: concurrent.Map[ServingLocation, ServingAgentDescriptor] = new ConcurrentHashMap[ServingLocation, ServingAgentDescriptor]().asScala

  val modelDefinitionMap: concurrent.Map[String, ServingModelDefinition] = new ConcurrentHashMap[String, ServingModelDefinition]().asScala

  def onAddServingAgent(agent: ServingHost): Unit = {
    servingAgentHosts.add(agent)
    if (LOG.isDebugEnabled) {
      LOG.debug(s"the agent:$agent is added")
    }
  }

  def onRemoveServingAgent(agent: ServingHost): Unit = {
    servingAgentHosts.remove(agent)
    val toRemoved = servingAgentLocs.filter(loc => loc.ip.equals(agent.ip)).toArray
    toRemoved.foreach(loc => {
      servingAgentLocs.remove(loc)
      servingAgentMap.remove(loc).foreach(servAgentDesc => servAgentDesc.exit())
    })
  }

  def registerServingAgent(agentLoc: ServingLocation): Unit = {
    if (servingAgentHosts.contains(agentLoc.host)) {
      servingAgentLocs.add(agentLoc)
      servingAgentMap.putIfAbsent(agentLoc, new ServingAgentDescriptor(agentLoc))
      LOG.info(s"the agent:${agentLoc.host} is registered, and the port is ${agentLoc.port}")
    }
  }

  def registerModel(model: ModelDefinition) = {
    require(modelDefinitionMap.put(model.name, new ServingModelDefinition(model)).isEmpty)
    LOG.info(s"the model:${model.name} is registered")
  }

  def unregisterModel(name: String) = {
    modelDefinitionMap.remove(name)
    servingAgentMap.foreach { case (_, agentDesc) => agentDesc.remove(name) }
    LOG.info(s"the model:${name} is unregistered")
  }

  def modelReport(modelReport: ModelReport): ModelCommand = {
    val loc = modelReport.servingLoc
    val servingAgentDesc = servingAgentMap.get(loc).orNull
    val unloadedSplits: ArrayBuffer[ModelSplitID] = ArrayBuffer.empty
    val forLoadingSplits: ArrayBuffer[ModelSplitGroup] = ArrayBuffer.empty
    require(servingAgentDesc != null, s"the agent:$loc is not register as serving agent")

    val loadedOnAgent = modelReport.loaded
    val loadingOnAgent = modelReport.loading

    val loadedOnAgentByName = loadedOnAgent.groupBy(_.name)
      .map {
        case (name, splitIDs) => {
          (name, splitIDs.map(_.index).toSet)
        }
      }

    val loadingOnAgentByName = loadingOnAgent.groupBy(_.name)
      .map {
        case (name, splitIDs) => {
          (name, splitIDs.map(_.index).toSet)
        }
      }



    // 1.0 process master loading
    servingAgentDesc.forLoadingModelDefinitionMap.foreach {
      case (name, splits) => {
        // 1.0 master loading change to loaded if agent is loaded
        loadedOnAgentByName.get(name).foreach(_.foreach(splits.get(_).foreach(loadedSplit => servingAgentDesc.finishLoad(name, loadedSplit.index))))
        // 2.0 compare with agent loading
        val loadingOnAgentModelOpt = loadingOnAgentByName.get(name)
        val model = modelDefinitionMap(name).modelDefinition
        if (loadingOnAgentModelOpt.isEmpty && !splits.isEmpty) {
          forLoadingSplits += new ModelSplitGroup(model.name, model.dir, model.concurrent, splits.values.toArray)
        } else {
          val needLoading = splits.values.filterNot(split => loadingOnAgentModelOpt.get.contains(split.index))
          if (needLoading.size > 0) {
            forLoadingSplits += new ModelSplitGroup(model.name, model.dir, model.concurrent, needLoading.toArray)
          }
        }
      }

    }

    // 2.0 process master loaded
    servingAgentDesc.loadedModelDefinitionMap.foreach {
      case (name, splits) => {
        val model = modelDefinitionMap(name).modelDefinition
        val loadedOnAgentByNameOpt = loadedOnAgentByName.get(name)

        // accident
        if (loadedOnAgentByNameOpt.isEmpty) {
          splits.values.foreach(split => {
            val idx = split.index
            LOG.warn(s"matrix[$idx] $name has loaded,but missing on agent: $loc")
            servingAgentDesc.removeSplit(name, idx)
            servingAgentDesc.addForLoading(name, split)
          })
          forLoadingSplits += new ModelSplitGroup(model.name, model.dir, model.concurrent, splits.values.toArray)
        } else {
          val needUnload = loadedOnAgentByNameOpt.get.filterNot(splits.contains(_)).map(idx => new ModelSplitID(name, idx))
          if (needUnload.size > 0) {
            unloadedSplits ++= needUnload
          }
        }
      }
    }

    val loadedModelOnMaster = servingAgentDesc.loadedModelDefinitionMap.keys.toSet
    val loadingModelOnMaster = servingAgentDesc.forLoadingModelDefinitionMap.keys.toSet

    //3.0 process agent exceed
    unloadedSplits ++= loadedOnAgentByName.filterNot { case (name, _) => (loadedModelOnMaster.contains(name) || loadingModelOnMaster.contains(name)) }
      .flatMap {
        case (name, splitIds) => splitIds.map(new ModelSplitID(name, _))
      }

    unloadedSplits ++= loadingOnAgentByName.filterNot { case (name, _) => (loadedModelOnMaster.contains(name) || loadingModelOnMaster.contains(name)) }
      .flatMap {
        case (name, splitIds) => splitIds.map(new ModelSplitID(name, _))
      }

    val command = new ModelCommand(forLoadingSplits.toArray, unloadedSplits.toArray)

    if (LOG.isDebugEnabled) {
      val loading = servingAgentDesc.forLoadingModelDefinitionMap.map { case (model, splits) => model + ":" + splits.keySet }.mkString(",")
      val loaded = servingAgentDesc.loadedModelDefinitionMap.map { case (model, splits) => model + ":" + splits.keySet }.mkString(",")
      LOG.debug(s"the agent:$loc loading:$loading ,loaded:$loaded")
    }
    command
  }

  def getModelLocations(): ModelLocationList = {
    val modelLocs = modelDefinitionMap.map { case (_, modelDef) => modelDef }
      .map(modelDef =>
        new ModelLocation(modelDef.modelDefinition.name, modelDef.loadedSplitLocs.map { case (idx, replica) => new ModelSplitLocation(idx, replica.locations) }.toArray))
    new ModelLocationList(modelLocs.toArray)
  }

  def start(): Unit = {
    monitor.start()
  }


  def exit(): Unit = {
    monitor.getRunnable.asInstanceOf[ServingModelMonitor].stop
    monitor.join()
    servingAgentLocs.clear()
    servingAgentMap.clear()
    modelDefinitionMap.clear()
  }

  class ServingModelMonitor(interval: Long) extends Runnable {

    private[this] var _stop: Boolean = false

    def stop: Boolean = _stop

    def stop_=(value: Boolean): Unit = {
      _stop = value
    }

    override def run(): Unit = {
      while (!stop) {
        try {
          val agentLocs = servingAgentLocs.clone()
          modelDefinitionMap.values.foreach(modelDef => {
            modelDef.splits.foreach(split => {
              val diff = modelDef.diff(split.index)
              if (diff > 0) {
                modelDef.askLoad(split.index, diff, modelDef.diff(split.index, agentLocs))
              } else {
                modelDef.askUnload(split.index, -diff)
              }
              if (LOG.isDebugEnabled) {
                val splitStr = split.index + ":" + split.replica.locations
                LOG.debug(s"the model:${modelDef.modelDefinition.name}'s split:$splitStr")
              }
            })
          })
          Thread.sleep(interval)
        } catch {
          case NonFatal(e) => LOG.error("model monitor occur error", e)
        }
      }
    }

    override def toString: String = "Model monitor"
  }

  class ServingAgentDescriptor(servingLoc: ServingLocation) {
    val loadedModelDefinitionMap: concurrent.Map[String, mutable.Map[Int, ModelSplit]] = new ConcurrentHashMap[String, mutable.Map[Int, ModelSplit]]().asScala

    val forLoadingModelDefinitionMap: concurrent.Map[String, mutable.Map[Int, ModelSplit]] = new ConcurrentHashMap[String, mutable.Map[Int, ModelSplit]]().asScala

    def remove(name: String): Unit = {
      forLoadingModelDefinitionMap.remove(name)
      loadedModelDefinitionMap.remove(name)
    }

    def exit(): Unit = {
      loadedModelDefinitionMap.foreach {
        case (name, splits) => splits.values.foreach(split => {
          val modelDef = modelDefinitionMap.get(name)
          modelDef.foreach(model => model.remove(split.index, servingLoc))
        })
      }
      forLoadingModelDefinitionMap.foreach {
        case (name, splits) => splits.values.foreach(split => {
          val modelDef = modelDefinitionMap.get(name)
          modelDef.foreach(model => model.remove(split.index, servingLoc))
        })
      }
    }

    def addForLoading(name: String, modelSplit: ModelSplit): Unit = {
      val modelSplitMap = forLoadingModelDefinitionMap.getOrElseUpdate(name, new mutable.HashMap[Int, ModelSplit]())
      modelSplitMap.getOrElseUpdate(modelSplit.index, modelSplit)
    }


    def removeSplit(name: String, modelSplit: Int): Option[ModelSplit] = {
      forLoadingModelDefinitionMap.get(name).flatMap(model => model.remove(modelSplit))
        .orElse(loadedModelDefinitionMap.get(name).flatMap(model => model.remove(modelSplit)))
    }

    def removeLoadingSplit(name: String, modelSplit: Int): Option[ModelSplit] = {
      forLoadingModelDefinitionMap.get(name).flatMap(model => model.remove(modelSplit))
    }

    def removeLoadedSplit(name: String, modelSplit: Int): Option[ModelSplit] = {
      loadedModelDefinitionMap.get(name).flatMap(model => model.remove(modelSplit))
    }


    def finishLoad(name: String, modelSplitIdx: Int): Unit = {
      forLoadingModelDefinitionMap.get(name).flatMap(model => {
        model.remove(modelSplitIdx)
      }).foreach(modelSplit => {
        loadedModelDefinitionMap.getOrElseUpdate(name, new mutable.HashMap[Int, ModelSplit]()).put(modelSplitIdx, modelSplit)
        modelDefinitionMap(name).finishLoad(modelSplitIdx, servingLoc)
      })
    }

    def get(loading: Boolean, name: String, modelSplitIdx: Int = -1): Option[Array[(Int, ModelSplit)]] = {
      val splitMap = if (loading) {
        forLoadingModelDefinitionMap
      } else {
        loadedModelDefinitionMap
      }
      if (modelSplitIdx > 0) {
        splitMap.get(name)
          .flatMap(model => model.get(modelSplitIdx))
          .map(split => Array((modelSplitIdx, split)))
      } else {
        splitMap.get(name).map(model => model.toArray)
      }
    }
  }

  class ServingModelDefinition(val modelDefinition: ModelDefinition) {

    lazy val splits: Array[ReplicaModelSplit] = modelDefinition.splits

    lazy val splitLocs: Map[Int, ServingReplica] = modelDefinition.splits.map(split => (split.index, split.replica)).toMap

    lazy val loadedSplitLocs: Map[Int, ServingReplica] = modelDefinition.splits.indices
      .map(splitIdx => (splitIdx, new ServingReplica())).toMap

    def askLoad(splitIdx: Int, replica: Int, servingLocs: Traversable[ServingLocation]): Unit = {
      val recommendLocs = Random.shuffle(servingLocs).toStream.take(replica)
      recommendLocs.foreach(loc => {
        splitLocs(splitIdx).addLoc(loc)
        servingAgentMap(loc).addForLoading(modelDefinition.name, modelDefinition.splits(splitIdx))
      })
    }

    def finishLoad(splitIdx: Int, servingLoc: ServingLocation): Unit = {
      require(splitLocs(splitIdx).locations.contains(servingLoc))
      loadedSplitLocs(splitIdx).addLoc(servingLoc)
    }


    def askUnload(splitIdx: Int, replica: Int): Unit = {
      val toUnloadLocs = splitLocs(splitIdx).remove(replica)
      if (toUnloadLocs.length > 0) {
        toUnloadLocs.foreach(
          loc => {
            servingAgentMap(loc).removeSplit(modelDefinition.name, splitIdx)
            loadedSplitLocs(splitIdx).removeLoc(loc)
          }
        )
      }
    }

    def remove(splitIdx: Int, servingLoc: ServingLocation): Unit = {
      splitLocs(splitIdx).removeLoc(servingLoc)
      loadedSplitLocs(splitIdx).removeLoc(servingLoc)
    }

    def replica(splitIdx: Int): Int = {
      splitLocs(splitIdx).locations.length
    }

    def diff(splitIdx: Int): Int = {
      modelDefinition.replica - replica(splitIdx)
    }

    def diff(splitIdx: Int, servingLocs: Traversable[ServingLocation]): Traversable[ServingLocation] = {
      val locs = splitLocs(splitIdx).locations.toSet
      servingLocs.filter(!locs.contains(_))
    }
  }

}






