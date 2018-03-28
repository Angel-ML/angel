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

package com.tencent.angel.utils;

import io.netty.buffer.*;

/**
 * Netty ByteBuf allocation utils.
 */
public class ByteBufUtils {
  private static volatile ByteBufAllocator allocator = null;
  public static volatile boolean useDirect = true;
  public static volatile boolean usePool = true;

  private static ByteBufAllocator getBufferAllocator() {
    if(allocator == null) {
      if(usePool) {
        allocator = new PooledByteBufAllocator();
      } else {
        allocator = new UnpooledByteBufAllocator(true);
      }
    }
    return allocator;
  }

  public static ByteBuf newHeapByteBuf(int estimizeSerilizeSize) {
    return getBufferAllocator().buffer(estimizeSerilizeSize);
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
}
