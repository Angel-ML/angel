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

package com.tencent.angel.transport

import java.util.concurrent.ConcurrentHashMap

import com.tencent.angel.exception.AngelException

import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.util.control.NonFatal

class ResponseMessageHandler extends MessageHandler[ResponseMessage] {
  private val callbacks: concurrent.Map[Long, ResponseCallback] = new ConcurrentHashMap[Long, ResponseCallback]().asScala

  def addCallback(id: Long, callback: ResponseCallback): Unit = {
    require(callbacks.put(id, callback).isEmpty)
  }

  def removeCallback(id: Long): Unit = {
    callbacks.remove(id)
  }

  override def handler(msg: ResponseMessage): Unit = {
    val callbackOpt = callbacks.remove(msg.id)
    try {
      msg match {
        case failureMsg: FailureMessage =>
          callbackOpt.foreach(_.onFailure(new AngelException("remote failed: " + FailureMessage.toError(failureMsg))))

        case _ =>
          callbackOpt.foreach(_.onSuccess(msg.body))
      }
    } catch {
      case NonFatal(e) => callbackOpt.foreach(_.onFailure(new AngelException("local failed", e)))
    } finally {
      msg.body.release()
    }
  }
}

object ResponseMessageHandler {
  val INSTANCE = new ResponseMessageHandler
}
