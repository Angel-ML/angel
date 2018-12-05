package com.tencent.angel.spark.ml.psf.embedding.bad;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class W2VPushPartitionParam extends PartitionUpdateParam {
  int[] indices;
  float[] deltas;
  int numNodePerRow;
  int dimension;
  int start;
  int length;
  ByteBuf buf;

  public W2VPushPartitionParam(int matrixId,
                               PartitionKey pkey,
                               int[] indices,
                               float[] deltas,
                               int numNodePerRow,
                               int start,
                               int length,
                               int dimension) {
    super(matrixId, pkey);
    this.indices = indices;
    this.numNodePerRow = numNodePerRow;
    this.start = start;
    this.dimension = dimension;
    this.length = length;
    this.deltas = deltas;
  }

  public W2VPushPartitionParam() { }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(numNodePerRow);
    buf.writeInt(dimension);
    buf.writeInt(length);
    for (int i = 0; i < length; i++) {
      buf.writeInt(indices[i + start]);
      int offset = (start + i) * dimension * 2;
      for (int j = 0; j < dimension * 2; j++)
        buf.writeFloat(deltas[offset + j]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    this.numNodePerRow = buf.readInt();
    this.dimension = buf.readInt();
    this.length = buf.readInt();
    this.buf = buf.duplicate();
    this.buf.retain();
  }

  public void clear() {
    buf.release();
  }


  @Override
  public int bufferLen() {
    return super.bufferLen();
  }
}
