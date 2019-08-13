package com.tencent.angel.spark.ml.psf.gcn;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class GetLabelsPartParam extends PartitionGetParam {
  private long[] nodes;
  private int startIndex;
  private int endIndex;

  public GetLabelsPartParam(int matrixId, PartitionKey partKey,
                            long[] nodes, int startIndex, int endIndex) {
    super(matrixId, partKey);
    this.nodes = nodes;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public GetLabelsPartParam() {
    super();
  }

  public long[] getNodes() {
    return nodes;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(endIndex - startIndex);
    for (int i = startIndex; i < endIndex; i++)
      buf.writeLong(nodes[i]);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int len = buf.readInt();
    nodes = new long[len];
    for (int i = 0; i < len; i++)
      nodes[i] = buf.readLong();
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4;
    len += 8 * (endIndex - startIndex);
    return len;
  }

}
