package com.tencent.angel.spark.ml.psf.embedding.bad;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class W2VPullParatitionParam extends PartitionGetParam {

  int[] indices;
  int numNodePerRow;
  int dimension;
  int start;
  int length;

  public W2VPullParatitionParam(int matrixId,
                                PartitionKey partKey,
                                int[] indices,
                                int numNodePerRow,
                                int start,
                                int length,
                                int dimension) {
    super(matrixId, partKey);
    this.indices = indices;
    this.numNodePerRow = numNodePerRow;
    this.start = start;
    this.dimension = dimension;
    this.length = length;
  }

  public W2VPullParatitionParam() { }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    numNodePerRow = buf.readInt();
    start = buf.readInt();
    dimension = buf.readInt();
    length = buf.readInt();
    indices = new int[length];
    for (int i = 0; i < length; i ++) indices[i] = buf.readInt();
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(numNodePerRow);
    buf.writeInt(start);
    buf.writeInt(dimension);
    buf.writeInt(length);
    for (int i = 0; i < length; i ++) buf.writeInt(indices[i + start]);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 4 + 4 * indices.length;
  }
}
