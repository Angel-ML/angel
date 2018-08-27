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


package com.tencent.angel.ml.matrix.codec;

import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DefaultDenseDoubleCodec implements DenseDoubleCodec {
  protected final static Log LOG = LogFactory.getLog(DefaultDenseDoubleCodec.class);

  @Override public void encode(ByteBuf outBuf, double[] values, int startPos, int length) {
    outBuf.writeInt(length);
    LOG.debug("double size = " + length);
    int end = startPos + length;
    for (int i = startPos; i < end; i++) {
      outBuf.writeDouble(values[i]);
    }
  }

  @Override public void decode(ByteBuf inBuf, double[] data, int startPos, int len) {
    int length = inBuf.readInt();
    for (int i = 0; i < length; i++) {
      data[i + startPos] = inBuf.readDouble();
    }
  }

  @Override public void serialize(ByteBuf buf) {

  }

  @Override public void deserialize(ByteBuf buf) {

  }

  @Override public int bufferLen() {
    return 0;
  }

}
