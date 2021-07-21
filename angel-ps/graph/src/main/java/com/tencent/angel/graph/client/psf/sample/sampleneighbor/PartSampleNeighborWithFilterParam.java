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
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import io.netty.buffer.ByteBuf;

public class PartSampleNeighborWithFilterParam extends PartitionGetParam {

  /**
   * Sample count
   */
  private int count;

  /**
   * Sample type
   */
  private SampleType sampleType;

  /**
   * Node id to filter with neighbor key
   */
  private KeyValuePart indicesToFilterPart;

  /**
   * filter without some neighbors while sample neighbors
   */
  private long[] filterWithoutNeighKeys;

  public PartSampleNeighborWithFilterParam(int matrixId, PartitionKey partKey, int count,
      SampleType sampleType, KeyValuePart indicesToFilterPart, long[] filterWithoutNeighKeys) {
    super(matrixId, partKey);
    this.count = count;
    this.sampleType = sampleType;
    this.indicesToFilterPart = indicesToFilterPart;
    this.filterWithoutNeighKeys = filterWithoutNeighKeys;
  }

  public PartSampleNeighborWithFilterParam() {
    this(-1, null, -1, SampleType.NODE, null, null);
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public SampleType getSampleType() {
    return sampleType;
  }

  public void setSampleType(SampleType sampleType) {
    this.sampleType = sampleType;
  }

  public KeyValuePart getIndicesToFilterPart() {
    return indicesToFilterPart;
  }

  public void setIndicesToFilterPart(
      KeyValuePart indicesToFilterPart) {
    this.indicesToFilterPart = indicesToFilterPart;
  }

  public long[] getFilterWithoutNeighKeys() {
    return filterWithoutNeighKeys;
  }

  public void setFilterWithoutNeighKeys(long[] filterWithoutNeighKeys) {
    this.filterWithoutNeighKeys = filterWithoutNeighKeys;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeInt(buf, count);
    ByteBufSerdeUtils.serializeInt(buf, sampleType.getIndex());
    ByteBufSerdeUtils.serializeKeyValuePart(buf, indicesToFilterPart);
    if(filterWithoutNeighKeys == null) {
      ByteBufSerdeUtils.serializeBoolean(buf, false);
    } else {
      ByteBufSerdeUtils.serializeBoolean(buf, true);
      ByteBufSerdeUtils.serializeLongs(buf, filterWithoutNeighKeys);
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    count = ByteBufSerdeUtils.deserializeInt(buf);
    sampleType = SampleType.valueOf(ByteBufSerdeUtils.deserializeInt(buf));
    indicesToFilterPart = ByteBufSerdeUtils.deserializeKeyValuePart(buf);
    if(ByteBufSerdeUtils.deserializeBoolean(buf)) {
      filterWithoutNeighKeys = ByteBufSerdeUtils.deserializeLongs(buf);
    }
  }

  @Override public int bufferLen() {
    int len = super.bufferLen();
    len += ByteBufSerdeUtils.INT_LENGTH;
    len += ByteBufSerdeUtils.INT_LENGTH;
    len += ByteBufSerdeUtils.serializedKeyValuePartLen(indicesToFilterPart);
    len += ByteBufSerdeUtils.BOOLEN_LENGTH;
    if(filterWithoutNeighKeys != null) {
      len += ByteBufSerdeUtils.serializedLongsLen(filterWithoutNeighKeys);
    }
    return len;
  }
}
