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
import com.tencent.angel.protobuf.generated.ClientServingServiceProtos.{LocationRequestProto, RegisterModelRequestProto, UnregisterModelRequestProto}
import com.tencent.angel.protobuf.generated.ServingProtos._
import com.tencent.angel.serving.ServingLocation
import com.tencent.angel.serving.common.{ModelDefinition, _}
import com.tencent.angel.serving.protocol.ClientProtocol
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._


/**
  * This class is the client side translator to translate the requests made on [[ClientProtocol]]
  * interfaces to the RPC server implementing [[ClientProtocolPB]].
  *
  * @param server server address
  * @param conf   the configuration
  */
class ClientProtocolClientTranslatorPB(server: InetSocketAddress, conf: Configuration) extends ClientProtocol {
  lazy val proxy: ClientProtocolPB = createProxy()


  def createProxy(): ClientProtocolPB = {
    TConnectionManager.getConnection(conf).getProtocol(server.getHostName, server.getPort, classOf[ClientProtocolPB], 0, null).asInstanceOf[ClientProtocolPB]
  }

  override def registerModel(model: ModelDefinition, shardingModelClass: String): Boolean = {
    def toMatrixSplitProto(matrixSplit: MatrixSplit): MatrixSplitProto = {
      MatrixSplitProto.newBuilder.setName(matrixSplit.name).setIndex(matrixSplit.idx).setRowOffset(matrixSplit.rowOffset).setRowNum(matrixSplit.rowNum).setColumnOffset(matrixSplit.columnOffset).setDimension(matrixSplit.dimension).build
    }

    val requestBuilder = RegisterModelRequestProto.newBuilder
    val modelBuilder = ModelDefinitionProto.newBuilder()
    modelBuilder.setName(model.name).setDir(model.dir).setReplica(model.replica).setConcurrent(model.concurrent)

    modelBuilder.addAllMatrixMetas(model.matrixMetas.map(meta => MatrixMetaInfoProto.newBuilder.setName(meta.name).setRowType(meta.rowType.getNumber).setRowNum(meta.rowType.getNumber).setRowNum(meta.rowType.getNumber).setRowNum(meta.rowNum).setDimension(meta.dimension).build()).toList.asJava)

    modelBuilder.addAllModelSplits(model.splits.map(split => ModelSplitProto.newBuilder.setIndex(split.index).addAllSplits(
      split.matrixSplits.values.map(toMatrixSplitProto).toList.asJava
    ).build).toList.asJava)
    requestBuilder.setModel(modelBuilder.build())
    requestBuilder.setShardingModelClass(shardingModelClass)
    val commonResponseProto = proxy.registerModel(null, requestBuilder.build)
    commonResponseProto.getSeemSucess
  }

  override def unregisterModel(name: String): Boolean = {
    val commonResponseProto = proxy.unregisterModel(null, UnregisterModelRequestProto.newBuilder.setModel(name).build)
    commonResponseProto.getSeemSucess
  }

  override def getModelLocations(): ModelLocationList = {
    val locsProto = proxy.getLocations(null, LocationRequestProto.newBuilder.build())
    val modelLocs = locsProto.getModelLocationListList.asScala.map(modelLoc => ModelLocation(modelLoc.getModel, modelLoc.getSplitLocationListList.asScala.map(splitLoc => ModelSplitLocation(splitLoc.getIndex, splitLoc.getLocationListList.asScala.map(loc => ServingLocation(loc.getIp, loc.getPort)).toArray)).toArray)).toArray
    ModelLocationList(modelLocs)
  }

}
