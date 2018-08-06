/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.transport

import java.nio.charset.StandardCharsets

import com.tencent.angel.common.location.Location
import com.tencent.angel.transport.MessageType.MessageType
import com.tencent.angel.utils.ByteBufUtils
import io.netty.buffer.ByteBuf
import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfterAll, FunSuite, Outcome}

class NetworkSuite extends FunSuite with BeforeAndAfterAll {
  val conf = new Configuration()

  final protected override def withFixture(test: NoArgTest): Outcome = {
    val testName = test.text
    val suiteName = this.getClass.getName
    try {
      println(s"\n\n===== TEST OUTPUT FOR $suiteName: '$testName' ======\n")
      test()
    } finally {
      println(s"\n===== FINISHED $suiteName: '$testName' ======\n")
    }
  }

  test("create server & client") {
    val handler = new BusinessMessageHandler() {
      override def handler(buf: ByteBuf, callback: ResponseCallback): Unit = {
        println(buf.readCharSequence(buf.readableBytes(), StandardCharsets.UTF_8))
        val bytes = "hello world,too".getBytes(StandardCharsets.UTF_8)
        val res = ByteBufUtils.newByteBuf(bytes.length).writeBytes(bytes)
        callback.onSuccess(res)
      }
    }
    val context = new NetworkContext(conf, handler)
    val server = context.createServer("localhost")
    val client = context.createClient()
    println(server.getPort)


    val future = FutureResponseCallback[String] {
      new Decoder[String] {
        override def decode(buf: ByteBuf): String = buf.readCharSequence(buf.readableBytes(), StandardCharsets.UTF_8).toString
      }
    }


    client.send(new Location("localhost", server.getPort), new RequestMessage {
      val bytes = "hello world".getBytes(StandardCharsets.UTF_8)
      val buf = ByteBufUtils.newByteBuf(bytes.length).writeBytes(bytes)

      override def msgType: MessageType = MessageType.Request

      override def body: ByteBuf = buf

      override def bodyLen: Int = buf.readableBytes()

      override def id: Long = 0
    },
      future)
    println(future.get)

  }


}
