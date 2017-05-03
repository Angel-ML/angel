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

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.internal.PlatformDependent;

import java.lang.reflect.Field;

public class NettyUtils {
  public static PooledByteBufAllocator createPooledByteBufAllocator(boolean allowDirectBufs,
      boolean allowCache, int numCores) {
    if (numCores == 0) {
      numCores = Runtime.getRuntime().availableProcessors();
    }
    return new PooledByteBufAllocator(allowDirectBufs && PlatformDependent.directBufferPreferred(),
        Math.min(getPrivateStaticField("DEFAULT_NUM_HEAP_ARENA"), numCores), Math.min(
            getPrivateStaticField("DEFAULT_NUM_DIRECT_ARENA"), allowDirectBufs ? numCores : 0),
        getPrivateStaticField("DEFAULT_PAGE_SIZE"), getPrivateStaticField("DEFAULT_MAX_ORDER"),
        allowCache ? getPrivateStaticField("DEFAULT_TINY_CACHE_SIZE") : 0,
        allowCache ? getPrivateStaticField("DEFAULT_SMALL_CACHE_SIZE") : 0,
        allowCache ? getPrivateStaticField("DEFAULT_NORMAL_CACHE_SIZE") : 0);
  }

  private static int getPrivateStaticField(String name) {
    try {
      Field f = PooledByteBufAllocator.DEFAULT.getClass().getDeclaredField(name);
      f.setAccessible(true);
      return f.getInt(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
