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

package com.tencent.angel.serving.transport.serving

import java.util.concurrent.TimeUnit

import com.google.common.util.concurrent.SettableFuture
import com.tencent.angel.common.location.Location
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math.TVector
import com.tencent.angel.serving.common.{ModelSplitID, PredictSplitData, PredictSplitDataList}
import com.tencent.angel.serving.{PredictResult, PredictResultList}
import com.tencent.angel.transport._
import io.netty.buffer.ByteBuf
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

import scala.util.control.NonFatal

/**
  * the serving transport client
  *
  * @param client
  * @param timeout
  */
class ServingTransportClient(client: NetworkClient, timeout: Long) {
  val LOG = LogFactory.getLog(classOf[ServingTransportClient])

  def predict[V <: TVector](data: PredictSplitData[V]): PredictResult = {
    val start = System.currentTimeMillis()
    val result = SettableFuture.create[PredictResMsg]()
    val channelOpt: Option[ChannelProxy] = getChannel(data.targets.map(loc => new Location(loc.ip, loc.port)))

    val request = new PredictReqMsg(data.splitID, data.data)

    val buf = ServingSplitMsgEncoder.encode(request)
    val message: CommonRequestMessage = new CommonRequestMessage(request.encodedLength, buf)
    client.sendWithChannel(channelOpt.get, message, new ResponseCallback {
      override def onFailure(e: Throwable): Unit = {
        result.setException(e)
      }

      override def onSuccess(response: ByteBuf): Unit = {
        try {
          //header
          val msgType = ServingType.decode(response)
          val id = ModelSplitID.decode(response)

          val msg = ServingSplitMsgDecoder.decode(msgType, id, response)
          result.set(msg.asInstanceOf[PredictResMsg])
        } catch {
          case NonFatal(e) => result.setException(e)
        }
      }
    })
    try {
      val res = result.get(timeout, TimeUnit.MILLISECONDS)
      val score = res.score
      if (LOG.isDebugEnabled) {
        val id = message.id
        val cost = System.currentTimeMillis() - start
        LOG.debug(s"predict request $id score:$score,cost:$cost ms")
      }
      new PredictResult(res.score)
    } catch {
      case NonFatal(e) => throw new AngelException("predict failed cost:" + (System.currentTimeMillis() - start) + " ms", e)
    }
  }

  def predict[V <: TVector](dataList: PredictSplitDataList[V]): PredictResultList = {
    val start = System.currentTimeMillis()
    val result = SettableFuture.create[BatchPredictResMsg]()


    val channelOpt: Option[ChannelProxy] = getChannel(dataList.targets.map(loc => new Location(loc.ip, loc.port)))
    val request = new BatchPredictReqMsg(dataList.splitID, dataList.data)

    val buf = ServingSplitMsgEncoder.encode(request)
    val message = new CommonRequestMessage(request.encodedLength, buf)
    client.sendWithChannel(channelOpt.get, message, new ResponseCallback {
      override def onFailure(e: Throwable): Unit = {
        result.setException(e)
      }

      override def onSuccess(response: ByteBuf): Unit = {
        //header
        val msgType = ServingType.decode(response)
        val id = ModelSplitID.decode(response)
        val msg = ServingSplitMsgDecoder.decode(msgType, id, response)
        result.set(msg.asInstanceOf[BatchPredictResMsg])
      }
    })
    try {
      val res = result.get(timeout, TimeUnit.MILLISECONDS)
      val scores = res.scores
      if (LOG.isDebugEnabled) {
        val id = message.id
        val cost = System.currentTimeMillis() - start
        LOG.debug(s"predict request $id scores:$scores,cost:$cost ms")
      }
      new PredictResultList(res.scores)
    } catch {
      case NonFatal(e) => throw new AngelException("predict failed cost:" + (System.currentTimeMillis() - start) + "ms", e)
    }
  }

  def getChannel(targets: Array[Location]): Option[ChannelProxy] = {
    var channelOpt: Option[ChannelProxy] = None
    for (loc <- targets if channelOpt.isEmpty) {
      val channelTry = client.getChannel(loc)
      if (channelTry.isFailure) {
        LOG.error(s"failed connect $loc", channelTry.failed.get)
      } else {
        channelOpt = Some(channelTry.get)
      }
    }

    if (channelOpt.isEmpty) {
      throw new AngelException(s"failed connect to serving agent,locs:${targets.mkString(",")}")
    }
    channelOpt
  }
}

object ServingTransportClient {

  def create(conf: Configuration, timeout: Long = 60 * 1000): ServingTransportClient = {
    val networkClient = new NetworkContext(conf).createClient()
    new ServingTransportClient(networkClient, timeout)
  }

}
