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
package com.tencent.angel.graph.client.sampleneighbor4;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class SampleNeighborPartParam extends PartitionGetParam {

  private long[] keys;
  private int numSample;
  private int startIndex;
  private int endIndex;
  private boolean sampleTypes;

  public SampleNeighborPartParam(int matrixId, PartitionKey pkey,
                                 int numSample, long[] keys,
                                 boolean sampleTypes,
                                 int startIndex, int endIndex) {
    super(matrixId, pkey);
    this.keys = keys;
    this.numSample = numSample;
    this.sampleTypes = sampleTypes;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
    assert endIndex > startIndex;
  }

  public SampleNeighborPartParam() {
    this(0, null, 0, null, false, 0, 0);
  }

  public long[] getKeys() {
    return keys;
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
    for (int i = startIndex; i < endIndex; i++)
      buf.writeLong(keys[i]);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    numSample = buf.readInt();
    sampleTypes = buf.readBoolean();
    keys = new long[buf.readInt()];
    for (int i = 0; i < keys.length; i++)
      keys[i] = buf.readLong();
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4 + 1 + 8 * (endIndex - startIndex);
    return len;
  }
}
