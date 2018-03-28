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

import java.util.concurrent._

import com.tencent.angel.serving._
import com.tencent.angel.serving.common.{ModelReport, ModelSplit, ModelSplitID}
import com.tencent.angel.serving.protocol.AgentProtocol
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.Daemon

import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.util.control.NonFatal

/**
  * the serving agent manager, manage the sharding model for serving service
  *
  * @param config      the configuration
  * @param servingHost the agent loc for serving
  * @param protocol    the protocol proxy which can communicate with serving master
  */
class ServingAgentManager(config: Configuration, val servingHost: ServingHost, protocol: AgentProtocol) {
  val LOG = LogFactory.getLog(classOf[ServingAgentManager])

  private[this] var _port: Int = _

  def port: Int = _port

  def port_=(value: Int): Unit = {
    _port = value
  }

  lazy val loc = new ServingLocation(servingHost.ip, port)

  val monitor: Daemon = new Daemon(new ServingMonitor(config.getLong("angel.serving.agent.heartbeat.interval", 3 * 1000)))

  val mutex: Object = new Object()

  val modelDefinitionMap: concurrent.Map[ModelSplitID, ShardingModel] = new ConcurrentHashMap[ModelSplitID, ShardingModel]().asScala

  val servingExecutors: concurrent.Map[String, ExecutorService] = new ConcurrentHashMap[String, ExecutorService]().asScala

  val loadingModelFutures: concurrent.Map[ModelSplitID, Future[ShardingModel]] = new ConcurrentHashMap[ModelSplitID, Future[ShardingModel]]().asScala

  lazy val pool = Executors.newFixedThreadPool(config.getInt("angel.serving.model.load.thread.num", Runtime.getRuntime.availableProcessors() / 2), Util.createThreadFactory("model loader pool -"))

  private def shutdownPools: Unit = {
    def shutdown(pool: ExecutorService) = {
      if (!pool.isShutdown) pool.shutdownNow()
    }

    shutdown(pool)
    servingExecutors.values.foreach(shutdown(_))
    if (LOG.isDebugEnabled) {
      LOG.debug("the pool is shutdown")
    }
  }

  sys.addShutdownHook(shutdownPools)


  def register(servingPort: Int): Unit = {
    port = servingPort
    protocol.registerAgent(loc)
    monitor.start()
    if (LOG.isDebugEnabled) {
      LOG.debug(servingHost + " is register, and the port is " + servingPort)
    }
  }

  def requestLoadModel(splitID: ModelSplitID, dir: String, modelSplit: ModelSplit, shardingModelClass: String): Unit = {
    loadingModelFutures.put(
      splitID,
      pool.submit(new ModelLoaderTask(splitID, dir, modelSplit, shardingModelClass, config))
    )
    LOG.info(s"$splitID is request to load, and the dir is $dir")
  }

  def requestUnloadModel(splitID: ModelSplitID): Unit = {
    val loadingFuture = loadingModelFutures.get(splitID)
    if (loadingFuture.isDefined) {
      loadingFuture.foreach{ future => future.cancel(true)}
      loadingModelFutures.remove(splitID)
      modelDefinitionMap.remove(splitID)
    } else {
      modelDefinitionMap.remove(splitID)
    }

    LOG.info(s"$splitID is request to unload")
  }


  def getModel(splitID: ModelSplitID): Option[ShardingModel] = {
    modelDefinitionMap.get(splitID)
  }


  def getExecutor(name: String): Option[ExecutorService] = {
    servingExecutors.get(name)
  }

  def exit(): Unit = {
    monitor.getRunnable.asInstanceOf[ServingMonitor].stop = true
    monitor.join()
    shutdownPools
    loadingModelFutures.clear()
    modelDefinitionMap.clear()
    LOG.info(s"the agent of $loc is exited")
  }


  class ModelLoaderTask(splitID: ModelSplitID, dir: String, modelSplit: ModelSplit, shardingModelClass:String, config: Configuration) extends Callable[ShardingModel] {
    override def call(): ShardingModel = {
      var model: ShardingModel = null
      try {
        val shardingMatrices = modelSplit.matrixSplits.map { case (name, matrixSplit) => (name, matrixSplit.load(dir, config)) }
        val modelClass = ModelFactory.get(splitID.name, shardingModelClass)
        model = ModelFactory.init(modelClass, shardingMatrices)
      } catch {
        case NonFatal(e) => {
          loadingModelFutures.remove(splitID)
          LOG.error("load mode failed", e)
        }
      }
      if (model != null) {
        mutex.synchronized {
          loadingModelFutures.remove(splitID)
          if (!Thread.interrupted()) {
            val optModel = modelDefinitionMap.put(splitID, model)
            require(optModel.isEmpty)
          }
        }
        if (LOG.isDebugEnabled) {
          LOG.debug(s"the agent of $loc is exited")
        }
        LOG.info(s"the model of $splitID is loaded")
      }
      model
    }
  }


  class ServingMonitor(interval: Long) extends Runnable {

    private[this] var _stop: Boolean = false

    def stop: Boolean = _stop

    def stop_=(value: Boolean): Unit = {
      _stop = value
    }

    override def run(): Unit = {
      while (!stop) {
        try {
          val modelReport = mutex.synchronized {
            new ModelReport(loc, modelDefinitionMap.keys.toArray, loadingModelFutures.keys.toArray)
          }
          val command = protocol.modelReport(modelReport)
          if (LOG.isDebugEnabled) {
            LOG.debug(s"agent of $loc report:$modelReport and receive command:$command")
          }
          command.forUnload.foreach(requestUnloadModel)
          command.forLoading.foreach(
            splitGroup => {
              val model = splitGroup.name
              val shardingModelClass = splitGroup.shardingModelClass
              if (splitGroup.concurrent > 1) {
                servingExecutors.getOrElseUpdate(model,
                  {
                    val executor = new ThreadPoolExecutor(0, splitGroup.concurrent, 60, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable], Util.createThreadFactory(s"$model serving pool"))
                    executor.allowCoreThreadTimeOut(true)
                    executor
                  })
              }
              splitGroup.splits.foreach(
                split =>
                  if (!loadingModelFutures.contains(new ModelSplitID(model, split.index))) {
                    requestLoadModel(new ModelSplitID(splitGroup.name, split.index), splitGroup.dir, split, shardingModelClass)
                  })
            })
        } catch {
          case e: InterruptedException => {
            stop = true
            LOG.error(s"$loc model report monitor interrupted", e)
          }
          case NonFatal(e) => LOG.error(s"$loc model report failed", e)
        }
        Thread.sleep(interval)
      }
    }
  }

}
