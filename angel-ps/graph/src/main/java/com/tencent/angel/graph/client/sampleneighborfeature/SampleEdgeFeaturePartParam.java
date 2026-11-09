package com.tencent.angel.graph.client.sampleneighborfeature;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class SampleEdgeFeaturePartParam extends PartitionGetParam {

  private long[] keys;
  private int numSample;
  private int startIndex;
  private int endIndex;
  private boolean sampleTypes;

  public SampleEdgeFeaturePartParam(int matrixId, PartitionKey pkey,
      int numSample, long[] keys, boolean sampleTypes,
      int startIndex, int endIndex) {
    super(matrixId, pkey);
    this.keys = keys;
    this.numSample = numSample;
    this.sampleTypes = sampleTypes;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
    assert endIndex > startIndex;
  }

  public SampleEdgeFeaturePartParam() {
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
    for (int i = startIndex; i < endIndex; i++) {
      buf.writeLong(keys[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    numSample = buf.readInt();
    sampleTypes = buf.readBoolean();
    keys = new long[buf.readInt()];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = buf.readLong();
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 8 + ByteBufSerdeUtils.serializedBooleanLen(sampleTypes) + 8 * (endIndex - startIndex);
    return len;
  }
}
