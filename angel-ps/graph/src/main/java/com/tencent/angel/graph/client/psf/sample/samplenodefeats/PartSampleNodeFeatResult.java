package com.tencent.angel.graph.client.psf.sample.samplenodefeats;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.data.Feature;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.element.ByteArrayElement;
import io.netty.buffer.ByteBuf;

public class PartSampleNodeFeatResult extends PartitionGetResult {

  /**
   * Partition id
   */
  public int partId;

  /**
   * Node feats
   */
  public ByteArrayElement[] feats;

  public PartSampleNodeFeatResult(int partId, ByteArrayElement[] feats) {
    this.partId = partId;
    this.feats = feats;
  }

  public PartSampleNodeFeatResult() {
    this(-1, null);
  }

  public ByteArrayElement[] getNodeFeats() {
    return feats;
  }

  public void setNodeFeats(ByteArrayElement[] feats) {
    this.feats = feats;
  }

  public int getPartId() {
    return partId;
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeInt(output, partId);
    ByteBufSerdeUtils.serializeInt(output, feats.length);
    for (ByteArrayElement feat : feats) {
      if (feat == null) {
        ByteBufSerdeUtils.serializeBoolean(output, true);
      } else {
        ByteBufSerdeUtils.serializeBoolean(output, false);
        feat.serialize(output);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = ByteBufSerdeUtils.deserializeInt(input);
    int size = ByteBufSerdeUtils.deserializeInt(input);
    feats = new ByteArrayElement[size];
    for (int i = 0; i < size; i++) {
      boolean isNull = ByteBufSerdeUtils.deserializeBoolean(input);
      if (!isNull) {
        feats[i] = new ByteArrayElement();
        feats[i].deserialize(input);
      }
    }
  }

  @Override
  public int bufferLen() {
    int len = 2 * ByteBufSerdeUtils.INT_LENGTH;
    for (ByteArrayElement feat : feats) {
      len += ByteBufSerdeUtils.BOOLEN_LENGTH;
      if (feat != null) {
        len += feat.bufferLen();
      }
    }
    return len;
  }
}
