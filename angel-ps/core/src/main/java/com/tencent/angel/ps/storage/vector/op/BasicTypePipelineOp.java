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


package com.tencent.angel.ps.storage.vector.op;

import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.KeyType;
import io.netty.buffer.ByteBuf;

/**
 * Pipeline operation interface for basic data type
 */
public interface BasicTypePipelineOp {

  /**
   * Get data use indices from storage
   *
   * @param keyType index type: int or long
   * @param indexSize index number
   * @param in input buffer that stored un-deserialized indices
   * @param out output buffer
   * @param func element init function, if not null, the element will be initialized by func
   */
  void indexGet(KeyType keyType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func);
}
