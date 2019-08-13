package com.tencent.angel.spark.ml.psf.pagerank;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class MyPullPartParam extends PartitionGetParam {
  private long[] keys;
  private int deltaId;
  private int sumId;
  private float resetProb;
  private float tol;
  private int startIndex;
  private int endIndex;

  public MyPullPartParam(int matrixId, PartitionKey partKey,
                         long[] keys, int deltaId, int sumId,
                         float resetProb, float tol,
                         int startIndex, int endIndex) {
    super(matrixId, partKey);
    this.keys = keys;
    this.deltaId = deltaId;
    this.sumId = sumId;
    this.resetProb = resetProb;
    this.tol = tol;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public MyPullPartParam() {
    this(-1, null, null, -1, -1, 0, 0, -1, -1);
  }

  public long[] getKeys() {
    return keys;
  }

  public int getDeltaId() {
    return deltaId;
  }

  public int getSumId() {
    return sumId;
  }

  public float getResetProb() {
    return resetProb;
  }

  public float getTol() {
    return tol;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(endIndex - startIndex);
    buf.writeInt(deltaId);
    buf.writeInt(sumId);
    buf.writeFloat(resetProb);
    buf.writeFloat(tol);
    long start = partKey.getStartCol();
    long end = partKey.getEndCol();
    long range = end - start;
    if (range < Integer.MAX_VALUE) {
      buf.writeByte(0); // int range
      for (int i = startIndex; i < endIndex; i++)
        buf.writeInt((int) (keys[i] - start));
    } else {
      buf.writeByte(1); // long range
      for (int i = startIndex; i < endIndex; i++)
        buf.writeLong(keys[i] - start);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int len = buf.readInt();
    deltaId = buf.readInt();
    sumId = buf.readInt();
    resetProb = buf.readFloat();
    tol = buf.readFloat();
    keys = new long[len];
    byte type = buf.readByte();
    switch (type) {
      case 0:
        for (int i = 0; i < len; i++)
          keys[i] = buf.readInt();
        break;
      case 1:
        for (int i = 0; i < len; i++)
          keys[i] = buf.readLong();
        break;
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4 + 4 + 4 + 4 + 4;
    if (partKey.getEndCol() - partKey.getStartCol() < Integer.MAX_VALUE)
      len += keys.length * 4;
    else
      len += keys.length * 8;
    return len;
  }
}
