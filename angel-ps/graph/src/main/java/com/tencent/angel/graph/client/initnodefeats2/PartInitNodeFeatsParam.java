package com.tencent.angel.graph.client.initnodefeats2;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.data.NodeUtils;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class PartInitNodeFeatsParam extends PartitionUpdateParam {

  private long[] nodeIds;
  private IntFloatVector[] feats;
  int startIndex;
  int endIndex;

  public PartInitNodeFeatsParam(int matrixId, PartitionKey partKey,
      long[] nodeIds, IntFloatVector[] feats, int startIndex, int endIndex) {
    super(matrixId, partKey);
    this.nodeIds = nodeIds;
    this.feats = feats;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public PartInitNodeFeatsParam() {
    this(-1, null, null, null, -1, -1);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public IntFloatVector[] getFeats() {
    return feats;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    int writeIndex = buf.writerIndex();
    int writeNum = 0;
    buf.writeInt(0);
    for (int i = startIndex; i < endIndex; i++) {
      if (feats[i] == null || feats[i].getSize() == 0) {
        continue;
      }
      buf.writeLong(nodeIds[i]);
      NodeUtils.serialize(feats[i], buf);
      writeNum++;
    }
    buf.setInt(writeIndex, writeNum);
  }


  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int len = buf.readInt();
    nodeIds = new long[len];
    feats = new IntFloatVector[len];

    for (int i = 0; i < len; i++) {
      nodeIds[i] = buf.readLong();
      feats[i] = NodeUtils.deserialize(buf);
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4;
    for (int i = startIndex; i < endIndex; i++) {
      if (feats[i] != null && feats[i].getSize() != 0) {
        len += 8;
        len += NodeUtils.dataLen(feats[i]);
      }

    }
    return len;
  }
}
