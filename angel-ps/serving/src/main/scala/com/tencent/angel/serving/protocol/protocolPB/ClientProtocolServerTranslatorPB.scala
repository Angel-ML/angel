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
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.protobuf.generated.{ClientServingServiceProtos, ServingProtos}
import com.tencent.angel.protobuf.generated.ClientServingServiceProtos.{LocationResponseProto, ModelLocationProto, ModelSplitLocationProto}
import com.tencent.angel.protobuf.generated.ServingProtos.{CommonResponseProto, ServingLocationProto}
import com.tencent.angel.serving.common._
import com.tencent.angel.serving.protocol.ClientProtocol

import scala.collection.JavaConverters._

/**
  * This class is used on the server side.
  * Calls come across the wire for the for protocol [[ClientProtocolPB]].
  * This class translates the PB data types to the native data types
  *
  * @param server the delegation of protocol
  */
class ClientProtocolServerTranslatorPB(server: ClientProtocol) extends ClientProtocolPB {

  override def getProtocolVersion(protocol: String, clientVersion: Long) = 0

  override def registerModel(controller: RpcController, request: ClientServingServiceProtos.RegisterModelRequestProto): CommonResponseProto = {

    val modelProto = request.getModel
    val metas = modelProto.getMatrixMetasList.asScala.map(metaProto => new MatrixMeta(metaProto.getName, RowType.valueOf(metaProto.getRowType), metaProto.getRowNum, metaProto.getDimension)).toArray

    val splits = modelProto.getModelSplitsList.asScala
      .map(modelSplitProto => new ReplicaModelSplit(modelSplitProto.getIndex, modelSplitProto.getSplitsList.asScala.map(
        matrixSplitProto => (matrixSplitProto.getName, MatrixSplit(matrixSplitProto.getName, matrixSplitProto.getIndex, matrixSplitProto.getRowOffset, matrixSplitProto.getRowNum, matrixSplitProto.getColumnOffset, matrixSplitProto.getDimension))
      ).toMap)).toArray

    val seemSucess = server.registerModel(
      new ModelDefinition(modelProto.getName, modelProto.getDir, modelProto.getConcurrent, modelProto.getReplica, metas, splits),
      request.getShardingModelClass
    )
    CommonResponseProto.newBuilder.setSeemSucess(seemSucess).build
  }

  override def unregisterModel(controller: RpcController, request: ClientServingServiceProtos.UnregisterModelRequestProto): CommonResponseProto = {
    val seemSucess = server.unregisterModel(request.getModel)
    CommonResponseProto.newBuilder.setSeemSucess(seemSucess).build
  }

  override def getLocations(controller: RpcController, request: ClientServingServiceProtos.LocationRequestProto): ClientServingServiceProtos.LocationResponseProto = {
    val modelLocsList = server.getModelLocations()

    def toModelSplitProto(modelSplitLoc: ModelSplitLocation): ModelSplitLocationProto = {
      ModelSplitLocationProto.newBuilder().setIndex(modelSplitLoc.idx)
        .addAllLocationList(
          modelSplitLoc.locs
            .map(loc => ServingLocationProto.newBuilder().setIp(loc.ip).setPort(loc.port).build()).toList.asJava).build
    }

    val modelLocs = modelLocsList.modelLocations.map(modelLoc => ModelLocationProto.newBuilder().setModel(modelLoc.name)
      .addAllSplitLocationList(modelLoc.splitLocations.map(toModelSplitProto(_)).toList.asJava).build()).toList.asJava
    LocationResponseProto.newBuilder().addAllModelLocationList(modelLocs).build()
  }
}
