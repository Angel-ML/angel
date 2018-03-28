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

package com.tencent.angel.serving.protocol.protocolPB

import java.net.InetSocketAddress

import com.tencent.angel.ipc.TConnectionManager
import com.tencent.angel.protobuf.generated.AgentServingServiceProtos.{ModelReportProto, ModelSplitIDProto}
import com.tencent.angel.protobuf.generated.ServingProtos.{MatrixSplitProto, ModelSplitProto, ServingLocationProto}
import com.tencent.angel.serving.ServingLocation
import com.tencent.angel.serving.common._
import com.tencent.angel.serving.protocol.AgentProtocol
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._

/**
  * This class is the client side translator to translate the requests made on [[AgentProtocol]]
  * interfaces to the RPC server implementing [[AgentProtocolPB]].
  *
  * @param server server address
  * @param conf   the configuration
  */
class AgentProtocolClientTranslatorPB(server: InetSocketAddress, conf: Configuration) extends AgentProtocol {
  lazy val proxy: AgentProtocolPB = createProxy()


  def createProxy(): AgentProtocolPB = {
    TConnectionManager.getConnection(conf).getProtocol(server.getHostName, server.getPort, classOf[AgentProtocolPB], 0, null).asInstanceOf[AgentProtocolPB]
  }

  override def registerAgent(agent: ServingLocation): Unit = {
    proxy.registerAgent(null, ServingLocationProto.newBuilder().setIp(agent.ip).setPort(agent.port).build())
  }

  override def modelReport(modelReport: ModelReport): ModelCommand = {
    val builder = ModelReportProto.newBuilder()
    builder.setLoc(ServingLocationProto.newBuilder().setIp(modelReport.servingLoc.ip).setPort(modelReport.servingLoc.port))

    if (modelReport.loaded != null) {
      modelReport.loaded.map(splitID => ModelSplitIDProto.newBuilder.setName(splitID.name).setIndex(splitID.index).build())
        .foreach(builder.addLoaded)
    }

    if (modelReport.loading != null) {
      modelReport.loading.map(splitID => ModelSplitIDProto.newBuilder.setName(splitID.name).setIndex(splitID.index).build())
        .foreach(builder.addLoading)
    }

    val commandProto = proxy.modelReport(null, builder.build())

    def toMatrixSplit(proto: MatrixSplitProto): MatrixSplit = {
      MatrixSplit(proto.getName, proto.getIndex, proto.getRowOffset, proto.getRowNum, proto.getColumnOffset, proto.getDimension)
    }

    def toModelSplit(proto: ModelSplitProto): ModelSplit = {
      new ModelSplit(proto.getIndex, proto.getSplitsList.asScala.map(matrixSplitProto => (matrixSplitProto.getName, toMatrixSplit(matrixSplitProto))).toMap)
    }


    val forLoad = commandProto.getForLoadingList.asScala.map{groupProto =>
      new ModelSplitGroup(
        groupProto.getName,
        groupProto.getDir,
        groupProto.getConcurrent,
        groupProto.getSplitsList.asScala.map(toModelSplit).toArray,
        groupProto.getShardingModelClass
      )
    }.toArray

    val forUnload = commandProto.getForUnloadList.asScala.map(proto => new ModelSplitID(proto.getName, proto.getIndex)).toArray

    new ModelCommand(forLoad, forUnload)
  }

}
