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
package com.tencent.angel.graph.client.sampleneighbor6;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class SampleNeighborPartParam extends PartitionGetParam {

  private long[] keys;
  private long[] withoutNeighKeys;//not contain some edges(neighbors), same len as keys
  private long[] containNeighKeys;//must contain some neighbors
  private int numSample;
  private int startIndex;
  private int endIndex;
  private boolean sampleTypes;
  private boolean withoutKey;
  private boolean containKey;

  public SampleNeighborPartParam(int matrixId, PartitionKey pkey,
                                 int numSample, long[] keys, long[] withoutNeighKeys, long[] containNeighKeys,
                                 boolean sampleTypes,
                                 int startIndex, int endIndex) {
    super(matrixId, pkey);
    this.keys = keys;
    this.withoutNeighKeys = withoutNeighKeys;
    this.containNeighKeys = containNeighKeys;
    this.numSample = numSample;
    this.sampleTypes = sampleTypes;
    this.withoutKey = withoutNeighKeys != null;
    this.containKey = containNeighKeys != null;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
    assert endIndex > startIndex;
  }

  public SampleNeighborPartParam() {
    this(0, null, 0, null, null, null,  false, 0, 0);
  }

  public long[] getKeys() {
    return keys;
  }

  public long[] getContainNeighKeys() {
    return containNeighKeys;
  }

  public long[] getWithoutNeighKeys() {
    return withoutNeighKeys;
  }

  public int getNumSample() {
    return numSample;
  }

  public int getStartIndex() {
    return startIndex;
  }

  public int getEndIndex() {
    return endIndex;
  }

  public boolean getSampleTypes() {
    return sampleTypes;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(numSample);
    buf.writeBoolean(sampleTypes);
    buf.writeInt(endIndex - startIndex);
    buf.writeBoolean(withoutKey);
    buf.writeBoolean(containKey);
    for (int i = startIndex; i < endIndex; i++) {
      buf.writeLong(keys[i]);
      if (withoutKey) {
        buf.writeLong(withoutNeighKeys[i]);
      }
    }

    if (containKey) {
      buf.writeInt(containNeighKeys.length);
      for (int i = 0; i < containNeighKeys.length; i++)
        buf.writeLong(containNeighKeys[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    numSample = buf.readInt();
    sampleTypes = buf.readBoolean();
    int keysLen = buf.readInt();
    keys = new long[keysLen];
    withoutKey = buf.readBoolean();
    containKey = buf.readBoolean();

    if (withoutKey) withoutNeighKeys = new long[keysLen];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = buf.readLong();
      if (withoutKey)
        withoutNeighKeys[i] = buf.readLong();
    }

    if (containKey) {
      containNeighKeys = new long[buf.readInt()];
      for (int i = 0; i < containNeighKeys.length; i++)
        containNeighKeys[i] = buf.readLong();
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 8 + ByteBufSerdeUtils.serializedBooleanLen(sampleTypes) * 3 + 8 * (endIndex - startIndex);
    if (withoutKey) {
      len += 8 * (endIndex - startIndex);
    }

    if (containKey) {
      len += 4 + containNeighKeys.length * 8;
    }
    return len;
  }
}
