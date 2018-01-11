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

import com.tencent.angel.exception.AngelException
import com.tencent.angel.serving.ShardingData
import com.tencent.angel.serving.agent.{AgentService, ServingAgentManager}
import com.tencent.angel.serving.common.ModelSplitID
import com.tencent.angel.serving.transport.serving.ServingType.ServingType
import com.tencent.angel.transport.{BusinessMessageHandler, ResponseCallback}
import io.netty.buffer.ByteBuf

import scala.util.control.NonFatal

/**
  * the implement of [[BusinessMessageHandler]] for handling with request serving message
  *
  * @param servingAgentService
  * @param servingAgentManager
  */
class ServingRequestMessageHandler(servingAgentService: AgentService, servingAgentManager: ServingAgentManager) extends BusinessMessageHandler {
  def handler(buf: ByteBuf, callback: ResponseCallback): Unit = {


    try {
      //header
      val msgType = ServingType.decode(buf)
      val splitID = ModelSplitID.decode(buf)


      val executorOpt = servingAgentManager.getExecutor(splitID.name)
      if (executorOpt.isDefined) {
        buf.retain
        executorOpt.foreach(executor => executor.submit(new Runnable {
          override def run(): Unit = {
            processMsg(msgType, splitID, buf, callback)
            buf.release
          }
        }))
      } else {
        processMsg(msgType, splitID, buf, callback)
      }
    }
    catch {
      case NonFatal(e) => callback.onFailure(e)
    }
  }

  // include decode
  def processMsg(msgType: ServingType, splitID: ModelSplitID, buf: ByteBuf, callback: ResponseCallback) = {
    try {
      val msg = ServingSplitMsgDecoder.decode(msgType, splitID, buf)
      msg match {
        case single: PredictReqMsg => {
          val ret = servingAgentService.predict(single.id, single.body)
          val response = new PredictResMsg(splitID, ret.score)
          callback.onSuccess(ServingSplitMsgEncoder.encode(response))
        }
        case batch: BatchPredictReqMsg[_] => {
          val ret = servingAgentService.batchPredict(batch.id, batch.body.data.map(chunk => chunk.asInstanceOf[ShardingData]))
          val response = new BatchPredictResMsg(splitID, ret.scores)
          callback.onSuccess(ServingSplitMsgEncoder.encode(response))
        }
        case _ => throw new AngelException("unsupported")
      }
    } catch {
      case NonFatal(e) => callback.onFailure(e)
    }
  }
}
