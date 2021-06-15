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

package com.tencent.angel.utils;

import com.tencent.angel.ps.server.data.response.IStreamResponse;
import com.tencent.angel.ps.server.data.response.Response;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.ps.server.data.response.ResponseType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Netty ByteBuf allocation utils.
 */
public class ByteBufUtils {

  private static final Log LOG = LogFactory.getLog(ByteBufUtils.class);
  private static volatile ByteBufAllocator allocator = null;
  public static volatile boolean useDirect = true;
  public static volatile boolean usePool = true;

  private static ByteBufAllocator getBufferAllocator() {
    if (allocator == null) {
      if (usePool) {
        allocator = new PooledByteBufAllocator();
      } else {
        allocator = new UnpooledByteBufAllocator(true);
      }
    }
    return allocator;
  }

  public static ByteBuf newHeapByteBuf(int estimizeSerilizeSize) {
    return getBufferAllocator().heapBuffer(estimizeSerilizeSize);
  }

  public static ByteBuf newDirectByteBuf(int estimizeSerilizeSize) {
    return getBufferAllocator().directBuffer(estimizeSerilizeSize);
  }

  public static ByteBuf newByteBuf(int estimizeSerilizeSize, boolean useDirect) {
    if (useDirect) {
      return newDirectByteBuf(estimizeSerilizeSize);
    } else {
      return newHeapByteBuf(estimizeSerilizeSize);
    }
  }

  public static ByteBuf newByteBuf(int estimizeSerilizeSize) {
    if (useDirect) {
      return newDirectByteBuf(estimizeSerilizeSize);
    } else {
      return newHeapByteBuf(estimizeSerilizeSize);
    }
  }

  public static void releaseBuf(ByteBuf in) {
    // Release the input buffer
    if (in.refCnt() > 0) {
      in.release();
    }
  }

  public static ByteBuf allocResultBuf(int size, boolean useDirectorBuffer) {
    ByteBuf buf;
    if (size > 16 * 1024 * 1024) {
      LOG.warn(
          "========================================Warning, allocate big buffer with size " + size);
    }
    buf = ByteBufUtils.newByteBuf(size, useDirectorBuffer);
    return buf;
  }

  public static ByteBuf allocResultBuf(Response response, boolean useDirect) {
    return allocResultBuf(response.bufferLen(), useDirect);
  }

  public static ByteBuf serializeResponse(Response response, boolean useDirectorBuffer) {
    ResponseData data = response.getData();

    // If is stream response, just return bytebuf
    if (data != null
        && (data instanceof IStreamResponse)
        && (((IStreamResponse) data).getOutputBuffer() != null)) {
      return ((IStreamResponse) data).getOutputBuffer();
    }

    ByteBuf buf = null;
    try {
      buf = allocResultBuf(response, useDirectorBuffer);
      response.serialize(buf);
    } catch (Throwable x) {
      LOG.error("serialize response failed ", x);
      if (buf != null) {
        buf.release();
      }

      if (response.getResponseType() == ResponseType.SUCCESS) {
        // Reset the response and allocate buffer again
        response.setResponseType(ResponseType.SERVER_IS_BUSY);
        response.setDetail("can not serialize the response");
        // Release response data
        response.setData(null);

        buf = allocResultBuf(response, useDirectorBuffer);
        response.serialize(buf);
      } else {
        throw x;
      }
    }
    return buf;
  }

}
