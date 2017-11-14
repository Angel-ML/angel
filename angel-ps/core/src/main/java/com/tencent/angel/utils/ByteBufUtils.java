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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * Netty ByteBuf allocation utils.
 */
public class ByteBufUtils {
  // private static UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
  private static PooledByteBufAllocator allocator = new PooledByteBufAllocator();
  public static volatile boolean useDirect = true;

  public static ByteBuf newHeapByteBuf(int estimizeSerilizeSize) {
    return allocator.buffer(estimizeSerilizeSize);
  }

  public static ByteBuf newDirectByteBuf(int estimizeSerilizeSize) {
    return allocator.directBuffer(estimizeSerilizeSize);
  }

  public static ByteBuf newByteBuf(int estimizeSerilizeSize, boolean useDirect) {
    if (useDirect) {
      return newDirectByteBuf(estimizeSerilizeSize);
    } else {
      return newByteBuf(estimizeSerilizeSize);
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
