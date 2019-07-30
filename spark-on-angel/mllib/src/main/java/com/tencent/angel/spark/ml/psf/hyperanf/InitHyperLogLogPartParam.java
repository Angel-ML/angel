package com.tencent.angel.spark.ml.psf.hyperanf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class InitHyperLogLogPartParam extends PartitionUpdateParam {

  private long[] nodes;
  private int p;
  private int sp;
  private transient int startIndex;
  private transient int endIndex;

  public InitHyperLogLogPartParam(int matrixId, PartitionKey pkey, long[] nodes,
                                  int startIndex, int endIndex, int p, int sp) {
    super(matrixId, pkey);
    this.nodes = nodes;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
    this.p = p;
    this.sp = sp;
  }

  public InitHyperLogLogPartParam() {
    this(0, null, null, 0, 0, 0, 0);
  }

  public long[] getNodes() {
    return nodes;
  }

  public int getP() {
    return p;
  }

  public int getSp() {
    return sp;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(p);
    buf.writeInt(sp);
    buf.writeInt(endIndex - startIndex);
    for (int i = startIndex; i < endIndex; i++)
      buf.writeLong(nodes[i]);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    p = buf.readInt();
    sp = buf.readInt();
    int len = buf.readInt();
    nodes = new long[len];
    for (int i = 0; i < len; i++)
      nodes[i] = buf.readLong();
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4 + 4;
    len += 4 + 8 * (endIndex - startIndex);
    return len;
  }
}
