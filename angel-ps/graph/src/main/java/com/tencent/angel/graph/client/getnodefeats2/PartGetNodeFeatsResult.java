package com.tencent.angel.graph.client.getnodefeats2;

import com.tencent.angel.graph.data.NodeUtils;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartGetNodeFeatsResult extends PartitionGetResult {

  private int partId;
  private IntFloatVector[] feats;

  public PartGetNodeFeatsResult(int partId, IntFloatVector[] feats) {
    this.partId = partId;
    this.feats = feats;
  }

  public PartGetNodeFeatsResult() {
    this(-1, null);
  }

  public int getPartId() {
    return partId;
  }

  public IntFloatVector[] getFeats() {
    return feats;
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(partId);
    output.writeInt(feats.length);
    for (int i = 0; i < feats.length; i++) {
      if (feats[i] == null) {
        output.writeBoolean(true);
      } else {
        output.writeBoolean(false);
        NodeUtils.serialize(feats[i], output);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = input.readInt();
    int len = input.readInt();
    feats = new IntFloatVector[len];
    for (int i = 0; i < len; i++) {
      boolean isNull = input.readBoolean();
      if(!isNull) {
        feats[i] = NodeUtils.deserialize(input);
      }
    }
  }

  @Override
  public int bufferLen() {
    int len = 8;
    for (int i = 0; i < feats.length; i++) {
      if (feats[i] == null) {
        len += 4;
      } else {
        len += 4;
        len += NodeUtils.dataLen(feats[i]) * 4;
      }
    }
    return len;
  }
}
