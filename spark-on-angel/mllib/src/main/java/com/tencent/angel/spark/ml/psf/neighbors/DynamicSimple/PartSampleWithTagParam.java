package com.tencent.angel.spark.ml.psf.neighbors.DynamicSimple;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartSampleWithTagParam extends PartitionGetParam {

  private long[] nodeIds;
  private int[] tags;

  public PartSampleWithTagParam(int matrixId, PartitionKey partKey, long[] nodeIds, int[] tags) {
    super(matrixId, partKey);
    this.nodeIds = nodeIds;
    this.tags = tags;
  }

  public PartSampleWithTagParam() {
    this(-1, null, null, null);
  }

  public PartSampleWithTagParam(int matrixId, PartitionKey partKey, long[] nodeIds) {
    super(matrixId, partKey);
    this.nodeIds = nodeIds;
    this.tags = null;
  }

  public long[] getNodeIds() { return nodeIds; }

  public int[] getTags() {return tags; }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeLongs(buf, nodeIds);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    nodeIds = ByteBufSerdeUtils.deserializeLongs(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedLongsLen(nodeIds);
  }

}
