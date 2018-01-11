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


import java.util.concurrent.TimeUnit

import com.google.common.util.concurrent.SettableFuture
import io.netty.buffer.ByteBuf

trait ResponseCallback {
  def onSuccess(response: ByteBuf): Unit

  def onFailure(e: Throwable): Unit

}

class FutureResponseCallback[T](decode: Decoder[T]) extends ResponseCallback {
  val result = SettableFuture.create[T]()

  override def onFailure(e: Throwable): Unit = {
    result.setException(e)
  }

  override def onSuccess(response: ByteBuf): Unit = {
    result.set(decode.decode(response))
  }

  def get: T = {
    result.get
  }

  def get(timeout: Long): Unit = {
    result.get(timeout, TimeUnit.SECONDS)
  }
}

object FutureResponseCallback {
  def apply[T](decode: Decoder[T]): FutureResponseCallback[T] = new FutureResponseCallback(decode)
}


