package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.data.NeighborStorageType;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import io.netty.buffer.ByteBuf;

public class PartSampleNeighborWithTimeParam extends PartitionGetParam {

  /**
   * Sample count
   */
  private int count;

  /**
   * Node id to filter with neighbor key
   */
  private KeyValuePart indicesToTimePart;

  /**
   * Sample method
   */
  private SampleMethod method;

  /**
   * Storage type of neighbor
   */
  private NeighborStorageType storageType;


  public PartSampleNeighborWithTimeParam(int matrixId, PartitionKey partKey,
      KeyValuePart indicesToTimePart,
      int count, SampleMethod method, NeighborStorageType storageType) {
    super(matrixId, partKey);
    this.count = count;
    this.indicesToTimePart = indicesToTimePart;
    this.method = method;
    this.storageType = storageType;
  }

  public PartSampleNeighborWithTimeParam() {
    this(-1, null, null, -1, SampleMethod.RANDOM, NeighborStorageType.LONGARRAY);
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public KeyValuePart getIndicesToTimePart() {
    return indicesToTimePart;
  }

  public void setIndicesToTimePart(
      KeyValuePart indicesToTimePart) {
    this.indicesToTimePart = indicesToTimePart;
  }

  public SampleMethod getMethod() {
    return method;
  }

  public void setMethod(SampleMethod method) {
    this.method = method;
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
    ByteBufSerdeUtils.serializeInt(buf, method.getIndex());
    ByteBufSerdeUtils.serializeKeyValuePart(buf, indicesToTimePart);
    ByteBufSerdeUtils.serializeByte(buf, storageType.getIndex());
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    count = ByteBufSerdeUtils.deserializeInt(buf);
    method = SampleMethod.valueOf(ByteBufSerdeUtils.deserializeInt(buf));
    indicesToTimePart = ByteBufSerdeUtils.deserializeKeyValuePart(buf);
    storageType = NeighborStorageType.valueOf(ByteBufSerdeUtils.deserializeByte(buf));
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += ByteBufSerdeUtils.INT_LENGTH;
    len += ByteBufSerdeUtils.INT_LENGTH;
    len += ByteBufSerdeUtils.serializedKeyValuePartLen(indicesToTimePart);
    len += ByteBufSerdeUtils.BYTE_LENGTH;
    return len;
  }
}
