package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.data.NeighborStorageType;
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
   * filter with some neighbors while sample neighbors
   */
  private long[] filterWithNeighKeys;

  private NeighborStorageType storageType;

  public PartSampleNeighborWithFilterParam(int matrixId, PartitionKey partKey, int count,
      SampleType sampleType, KeyValuePart indicesToFilterPart, long[] filterWithNeighKeys,
      NeighborStorageType storageType) {
    super(matrixId, partKey);
    this.count = count;
    this.sampleType = sampleType;
    this.indicesToFilterPart = indicesToFilterPart;
    this.filterWithNeighKeys = filterWithNeighKeys;
    this.storageType = storageType;
  }

  public PartSampleNeighborWithFilterParam() {
    this(-1, null, -1, SampleType.NODE, null, null, NeighborStorageType.LONGARRAY);
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

  public long[] getFilterWithNeighKeys() {
    return filterWithNeighKeys;
  }

  public void setFilterWithNeighKeys(long[] filterWithNeighKeys) {
    this.filterWithNeighKeys = filterWithNeighKeys;
  }

  public NeighborStorageType getStorageType() {
    return storageType;
  }

  public void setStorageType(NeighborStorageType storageType) {
    this.storageType = storageType;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeInt(buf, count);
    ByteBufSerdeUtils.serializeInt(buf, sampleType.getIndex());
    ByteBufSerdeUtils.serializeKeyValuePart(buf, indicesToFilterPart);
    if (filterWithNeighKeys == null) {
      ByteBufSerdeUtils.serializeBoolean(buf, false);
    } else {
      ByteBufSerdeUtils.serializeBoolean(buf, true);
      ByteBufSerdeUtils.serializeLongs(buf, filterWithNeighKeys);
    }
    ByteBufSerdeUtils.serializeByte(buf, storageType.getIndex());
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    count = ByteBufSerdeUtils.deserializeInt(buf);
    sampleType = SampleType.valueOf(ByteBufSerdeUtils.deserializeInt(buf));
    indicesToFilterPart = ByteBufSerdeUtils.deserializeKeyValuePart(buf);
    if (ByteBufSerdeUtils.deserializeBoolean(buf)) {
      filterWithNeighKeys = ByteBufSerdeUtils.deserializeLongs(buf);
    }
    storageType = NeighborStorageType.valueOf(ByteBufSerdeUtils.deserializeByte(buf));
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += ByteBufSerdeUtils.INT_LENGTH;
    len += ByteBufSerdeUtils.INT_LENGTH;
    len += ByteBufSerdeUtils.serializedKeyValuePartLen(indicesToFilterPart);
    len += ByteBufSerdeUtils.BOOLEN_LENGTH;
    if (filterWithNeighKeys != null) {
      len += ByteBufSerdeUtils.serializedLongsLen(filterWithNeighKeys);
    }
    len += ByteBufSerdeUtils.BYTE_LENGTH;
    return len;
  }
}
