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

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.serving.common.{ModelMeta, _}
import com.tencent.angel.serving.protocol.ClientProtocol
import com.tencent.angel.serving.transport.serving.ServingTransportClient
import com.tencent.angel.tools.ModelLoader
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.util.Try

/**
  * the implement of {@link ServingClient}
  *
  * @param clientService   the client service
  * @param transportClient the transport client
  * @param config          the configuration
  */
class ServingClientImpl(clientService: ClientProtocol, transportClient: ServingTransportClient, config: Configuration) extends ServingClient {
  val LOG: Log = LogFactory.getLog(classOf[ServingClientImpl])

  val servingManager = new ClientServingManager(clientService, config)

  def getServingManager(): ClientServingManager = servingManager

  override def loadModel(name: String,
                         dir: String,
                         servingNodes: Int,
                         concurrent: Int,
                         splitter: ModelSplitter,
                         coordinator: ModelCoordinator,
                         shardingModelClass: String
  ): Boolean = {
    require(!servingManager.isRegistered(name), s"$name is loaded")
    val modelMeta = getModelMetaInfo(name, dir)
    val modelFormat = new ModelFormat(name, dir, concurrent, servingNodes, modelMeta, splitter, coordinator)
    loadModel(modelFormat, shardingModelClass)
  }

  override def loadModel(name: String, dir: String, replica: Int, concurrent: Int, shardingModelClass: String): Boolean = {
    val splitter = new DefaultModelSplitter(config.getInt(AngelConf.ANGEL_SERVING_SHARDING_NUM, 1))
    val coordinator = new DefaultModelCoordinator(name, this)

    val psNum = config.getInt(AngelConf.ANGEL_PS_NUMBER, 1)
    val replicaReal = if (replica > psNum) psNum else replica
    loadModel(name, dir, replicaReal, concurrent, splitter, coordinator, shardingModelClass)
  }

  override def loadModelLocal(name: String, dir: String, replica: Int, concurrent: Int): Unit = {
    require(!servingManager.isRegistered(name), s"$name is loaded")
    val splitter = new DefaultModelSplitter(config.getInt(AngelConf.ANGEL_SERVING_SHARDING_NUM, 1))
    val coordinator = new DefaultModelCoordinator(name, this)
    val modelMeta = getModelMetaInfo(name, dir)

    val psNum = config.getInt(AngelConf.ANGEL_PS_NUMBER, 1)
    val replicaReal = if (replica > psNum) psNum else replica
    val modelFormat = new ModelFormat(name, dir, concurrent, replicaReal, modelMeta, splitter, coordinator)
    loadModel(modelFormat, null, isRemoteLoad = false)
  }

  private[this] def getModelMetaInfo(name: String, dir: String): ModelMeta = {
    val path = new Path(dir)
    val fs = path.getFileSystem(config)
    require(fs.isDirectory(path), s"$dir not found or not directory")

    val matricesMeta = fs.listStatus(path).filter(_.isDirectory)
      .map(fileStatus => Try(ModelLoader.getMeta(fileStatus.getPath.toString, config)))
      .filter(_.isSuccess)
      .map(_.get)
      .map(fileMeta => MatrixMeta(fileMeta.getMatrixName, RowType.valueOf(fileMeta.getRowType), fileMeta.getRow, fileMeta.getCol))

    ModelMeta(matricesMeta)
  }

  private[this] def loadModel(modelFormat: ModelFormat, shardingModelClass: String, isRemoteLoad: Boolean = true): Boolean = {
    val model = modelFormat.getModel()
    servingManager.register(model)

    if (isRemoteLoad) {
      val definition = model.toModelDefinition()
      definition.validate()
      val seemSucess = clientService.registerModel(definition, shardingModelClass)
      LOG.info(s"${model.name} ask to load")
      seemSucess
    } else {
      LOG.info(s"${model.name} does not remotely loaded, only load a local model handle")
      true
    }
  }


  override def isServable(name: String): Boolean = {
    servingManager.isServable(name)
  }

  override def unloadModel(name: String): Boolean = {
    val seemSuccess = clientService.unregisterModel(name)
    servingManager.unregister(name)
    LOG.info(s"$name ask to unload")
    seemSuccess
  }

  override def getModel(name: String): Option[DistributedModel] = {
    servingManager.getModel(name)
  }

  override def getRouter(name: String): ModelRouter = {
    val model = getModel(name).orNull
    require(model != null, s"model:$name not found")
    new DefaultModelRouter(name, this, transportClient)
  }
}
