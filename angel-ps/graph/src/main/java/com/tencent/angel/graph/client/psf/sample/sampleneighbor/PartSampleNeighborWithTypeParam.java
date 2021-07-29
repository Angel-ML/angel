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

package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import io.netty.buffer.ByteBuf;

public class PartSampleNeighborWithTypeParam extends PartSampleNeighborParam {

  /**
   * Sample type
   */
  private SampleType sampleType;

  public PartSampleNeighborWithTypeParam(int matrixId, PartitionKey part, KeyPart indicesPart,
      int count, SampleType sampleType) {
    super(matrixId, part, indicesPart, count);
    this.sampleType = sampleType;
  }

  public PartSampleNeighborWithTypeParam() {
    this(-1, null, null, -1, SampleType.SIMPLE);
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeInt(buf, sampleType.getIndex());
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    sampleType = SampleType.valueOf(ByteBufSerdeUtils.deserializeInt(buf));
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.INT_LENGTH;
  }

  public SampleType getSampleType() {
    return sampleType;
  }

  public void setSampleType(SampleType sampleType) {
    this.sampleType = sampleType;
  }
}
