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

import com.google.protobuf.RpcController
import com.tencent.angel.protobuf.generated.AgentServingServiceProtos.{ModelCommandProto, ModelSplitGroupProto, ModelSplitIDProto}
import com.tencent.angel.protobuf.generated.ServingProtos.{CommonResponseProto, MatrixSplitProto, ModelSplitProto}
import com.tencent.angel.protobuf.generated.{AgentServingServiceProtos, ServingProtos}
import com.tencent.angel.serving.ServingLocation
import com.tencent.angel.serving.common.{MatrixSplit, ModelReport, ModelSplit, ModelSplitID}
import com.tencent.angel.serving.protocol.AgentProtocol

import scala.collection.JavaConverters._

/**
  * This class is used on the server side.
  * Calls come across the wire for the for protocol [[AgentProtocolPB]].
  * This class translates the PB data types to the native data types
  *
  * @param server the delegation of protocol
  */
class AgentProtocolServerTranslatorPB(server: AgentProtocol) extends AgentProtocolPB {

  override def getProtocolVersion(protocol: String, clientVersion: Long) = 0

  override def registerAgent(controller: RpcController, request: ServingProtos.ServingLocationProto): CommonResponseProto = {
    server.registerAgent(new ServingLocation(request.getIp, request.getPort))
    CommonResponseProto.newBuilder().build()
  }

  override def modelReport(controller: RpcController, request: AgentServingServiceProtos.ModelReportProto): ModelCommandProto = {
    val loaded = request.getLoadedList.asScala.map(splitIDProto => new ModelSplitID(splitIDProto.getName, splitIDProto.getIndex)).toArray

    val loading = request.getLoadingList.asScala.map(splitIDProto => new ModelSplitID(splitIDProto.getName, splitIDProto.getIndex)).toArray


    def toMatrixSplitProto(matrixSplit: MatrixSplit): MatrixSplitProto = {
      MatrixSplitProto.newBuilder.setName(matrixSplit.name).setIndex(matrixSplit.idx).setRowOffset(matrixSplit.rowOffset).setRowNum(matrixSplit.rowNum).setColumnOffset(matrixSplit.columnOffset).setDimension(matrixSplit.dimension).build
    }

    def toModelSplitProto(proto: ModelSplit): ModelSplitProto = {
      val builder = ModelSplitProto.newBuilder().setIndex(proto.index)
      proto.matrixSplits.values.map(toMatrixSplitProto).foreach(builder.addSplits)
      builder.build
    }

    val command = server.modelReport(new ModelReport(new ServingLocation(request.getLoc.getIp, request.getLoc.getPort), loaded, loading))

    val builder = ModelCommandProto.newBuilder

    val forLoad = command.forLoading.map { group =>
      ModelSplitGroupProto.newBuilder()
        .setName(group.name)
        .setDir(group.dir)
        .setConcurrent(group.concurrent)
        .addAllSplits(group.splits.map(toModelSplitProto).toIterable.asJava)
        .setShardingModelClass(group.shardingModelClass)
        .build
    }.toList.asJava
    val forUnload = command.forUnload.map(splitID => ModelSplitIDProto.newBuilder.setName(splitID.name).setIndex(splitID.index).build()).toList.asJava
    builder.addAllForLoading(forLoad).addAllForUnload(forUnload).build()
  }


}
