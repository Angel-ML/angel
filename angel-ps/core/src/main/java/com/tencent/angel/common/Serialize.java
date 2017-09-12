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

package com.tencent.angel.common;

import io.netty.buffer.ByteBuf;

import java.util.Queue;

/**
 * Serialize interface. It used by Netty to transfer data.
 */
public interface Serialize {
  /**
   * Serialize object to the Netty ByteBuf.
   * 
   * @param buf the Netty ByteBuf
   */
  void serialize(ByteBuf buf);

  /**
   * Deserialize object from the Netty ByteBuf.
   * 
   * @param buf the Netty ByteBuf
   */
  void deserialize(ByteBuf buf);

  /**
   * Estimate serialized data size of the object, it used to ByteBuf allocation.
   * 
   * @return int serialized data size of the object
   */
  int bufferLen();
}
