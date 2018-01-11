/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.tencent.angel.ipc;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Data structure, encoder and decoder classes for the Netty transport.
 */
public class NettyTransportCodec {
  private static final Logger LOG = LoggerFactory.getLogger(NettyTransportCodec.class.getName());
  /**
   * Transport protocol data structure when using Netty.
   */
  public static class NettyDataPack {
    private int serial; // to track each call in client side
    private List<ByteBuffer> datas;

    public NettyDataPack() {
    }

    public NettyDataPack(int serial, List<ByteBuffer> datas) {
      this.serial = serial;
      this.datas = datas;
    }

    public void setSerial(int serial) {
      this.serial = serial;
    }

    public int getSerial() {
      return serial;
    }

    public void setDatas(List<ByteBuffer> datas) {
      this.datas = datas;
    }

    public List<ByteBuffer> getDatas() {
      return datas;
    }
    @Override
    public String toString() {
      return Objects
          .toStringHelper(this)
          .add("serial", serial)
          .add("listSize", datas.size()).toString();
    }

    /**
     * Writes a NettyDataPack, reconnecting to the remote peer if necessary.
     * NOTE: The stateLock read lock *must* be acquired before calling this method.
     *
     * @param dataPack the data pack to write.
     * @throws java.io.IOException if an error occurs connecting to the remote peer.
     */
    public static void writeDataPack(Channel channel, NettyDataPack dataPack) throws IOException {
      channel.writeAndFlush(dataPack).addListener(future -> {
        if (future.isSuccess()) {
          LOG.trace("Sent result {} to client {}", dataPack, NettyUtils.getRemoteAddress(channel));
        } else {
          String msg = String.format("Error sending result %s to %s; closing connection",
              dataPack, NettyUtils.getRemoteAddress(channel));
          LOG.error(msg, future.cause());
          throw new IOException(msg, future.cause());
        }
      });
    }
  }

  /**
   * Protocol encoder which converts NettyDataPack which contains the Responder's output
   * List&lt;ByteBuffer&gt; to ChannelBuffer needed by Netty.
   */
  @ChannelHandler.Sharable
  public static class NettyFrameEncoder extends MessageToMessageEncoder<NettyDataPack> {
    public static final NettyFrameEncoder INSTANCE = new NettyFrameEncoder();
    /**
     * encode msg to ChannelBuffer
     *
     * @param dataPack NettyDataPack from NettyServerAvroHandler/NettyClientAvroHandler in the pipeline
     * @return encoded ChannelBuffer
     */
    @Override
    public void encode(ChannelHandlerContext ctx, NettyDataPack dataPack, List<Object> out) throws Exception {
      List<ByteBuffer> origs = dataPack.getDatas();
      int sumLen = 8 + 4 + 4;
      for (ByteBuffer b : origs) {
        sumLen += b.remaining() + 4;
      }
      ByteBuf buffer = ctx.alloc().heapBuffer(sumLen);
      buffer.writeLong(sumLen);
      buffer.writeInt(dataPack.getSerial());
      buffer.writeInt(dataPack.getDatas().size());
      for (ByteBuffer b : origs) {
        buffer.writeInt(b.remaining());
        buffer.writeBytes(b);
      }
      assert buffer.writableBytes() == 0;
      out.add(buffer);
    }
  }

  /**
   * Protocol decoder which converts Netty's ChannelBuffer to NettyDataPack which contains a
   * List&lt;ByteBuffer&gt; needed by Avro Responder.
   */
  @ChannelHandler.Sharable
  public static class NettyFrameDecoder extends MessageToMessageDecoder<ByteBuf> {
    public static final NettyFrameDecoder INSTANCE = new NettyFrameDecoder();
    /**
     * decode buffer to NettyDataPack
     */
    @Override
    public void decode(io.netty.channel.ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
      int serial = in.readInt();
      int listSize = in.readInt();
      List<ByteBuffer> datas = new ArrayList<ByteBuffer>(listSize);

      for (int i = 0; i < listSize; i++) {
        int len = in.readInt();
        ByteBuffer bb = ByteBuffer.allocate(len);
        in.readBytes(bb);
        bb.flip();
        datas.add(bb);
      }
      out.add(new NettyDataPack(serial, datas));
    }
  }
}
