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


package com.tencent.angel.psagent.matrix.transport.adapter;

import io.netty.buffer.ByteBuf;

public class LongIndicesView extends IndicesView {
  public final long[] indices;

  public LongIndicesView(long[] indices, int startPos, int endPos) {
    super(startPos, endPos);
    this.indices = indices;
  }

  @Override public void serialize(ByteBuf buf) {
    buf.writeInt(endPos - startPos);
    for (int i = startPos; i < endPos; i++) {
      buf.writeLong(indices[i]);
    }
  }

  @Override public void deserialize(ByteBuf buf) {

  }

  @Override public int bufferLen() {
    return 4 + 8 * (endPos - startPos);
  }
}
