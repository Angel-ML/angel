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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Data structure, encoder and decoder classes for the Netty transport.
 */
public class NettyTransportCodec {
  /**
   * Transport protocol data structure when using Netty.
   */
  public static class NettyDataPack {
    private int serial; // to track each call in client side
    private List<ByteBuffer> datas;

    public NettyDataPack() {}

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

  }

  /**
   * Protocol encoder which converts NettyDataPack which contains the Responder's output
   * List&lt;ByteBuffer&gt; to ChannelBuffer needed by Netty.
   */
  public static class NettyFrameEncoder extends OneToOneEncoder {

    /**
     * encode msg to ChannelBuffer
     * 
     * @param msg NettyDataPack from NettyServerAvroHandler/NettyClientAvroHandler in the pipeline
     * @return encoded ChannelBuffer
     */
    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg)
        throws Exception {
      NettyDataPack dataPack = (NettyDataPack) msg;
      List<ByteBuffer> origs = dataPack.getDatas();
      List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(origs.size() * 2 + 1);
      bbs.add(getPackHeader(dataPack)); // prepend a pack header including
                                        // serial number and list size
      for (ByteBuffer b : origs) {
        bbs.add(getLengthHeader(b)); // for each buffer prepend length field
        bbs.add(b);
      }

      return ChannelBuffers.wrappedBuffer(bbs.toArray(new ByteBuffer[bbs.size()]));
    }

    private ByteBuffer getPackHeader(NettyDataPack dataPack) {
      ByteBuffer header = ByteBuffer.allocate(8);
      header.putInt(dataPack.getSerial());
      header.putInt(dataPack.getDatas().size());
      header.flip();
      return header;
    }

    private ByteBuffer getLengthHeader(ByteBuffer buf) {
      ByteBuffer header = ByteBuffer.allocate(4);
      header.putInt(buf.limit());
      header.flip();
      return header;
    }
  }

  /**
   * Protocol decoder which converts Netty's ChannelBuffer to NettyDataPack which contains a
   * List&lt;ByteBuffer&gt; needed by Avro Responder.
   */
  public static class NettyFrameDecoder extends FrameDecoder {
    private boolean packHeaderRead = false;
    private int listSize;
    private NettyDataPack dataPack;

    /**
     * decode buffer to NettyDataPack
     */
    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer)
        throws Exception {

      if (!packHeaderRead) {
        if (decodePackHeader(ctx, channel, buffer)) {
          packHeaderRead = true;
        }
        return null;
      } else {
        if (decodePackBody(ctx, channel, buffer)) {
          packHeaderRead = false; // reset state
          return dataPack;
        } else {
          return null;
        }
      }

    }

    private boolean decodePackHeader(ChannelHandlerContext ctx, Channel channel,
        ChannelBuffer buffer) throws Exception {
      if (buffer.readableBytes() < 8) {
        return false;
      }

      int serial = buffer.readInt();
      listSize = buffer.readInt();
      dataPack = new NettyDataPack(serial, new ArrayList<ByteBuffer>(listSize));
      return true;
    }

    private boolean decodePackBody(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer)
        throws Exception {
      if (buffer.readableBytes() < 4) {
        return false;
      }

      buffer.markReaderIndex();

      int length = buffer.readInt();

      if (buffer.readableBytes() < length) {
        buffer.resetReaderIndex();
        return false;
      }

      ByteBuffer bb = ByteBuffer.allocate(length);
      buffer.readBytes(bb);
      bb.flip();
      dataPack.getDatas().add(bb);

      return dataPack.getDatas().size() == listSize;
    }
  }
}
