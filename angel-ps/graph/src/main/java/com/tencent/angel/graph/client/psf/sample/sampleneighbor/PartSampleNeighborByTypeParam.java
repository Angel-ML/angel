package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import io.netty.buffer.ByteBuf;

public class PartSampleNeighborByTypeParam extends PartSampleNeighborParam {

  /**
   * Sample type
   */
  private SampleType sampleType;
  private int node_or_edge_type;

  public PartSampleNeighborByTypeParam(int matrixId, PartitionKey part, KeyPart indicesPart,
      int count, SampleType sampleType, int node_or_edge_type) {
    super(matrixId, part, indicesPart, count);
    this.sampleType = sampleType;
    this.node_or_edge_type = node_or_edge_type;
  }

  public PartSampleNeighborByTypeParam() {
    this(-1, null, null, -1, SampleType.SIMPLE, -1);
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeInt(buf, sampleType.getIndex());
    ByteBufSerdeUtils.serializeInt(buf, node_or_edge_type);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    sampleType = SampleType.valueOf(ByteBufSerdeUtils.deserializeInt(buf));
    node_or_edge_type = ByteBufSerdeUtils.deserializeInt(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.INT_LENGTH * 2;
  }

  public SampleType getSampleType() {
    return sampleType;
  }

  public void setSampleType(SampleType sampleType) {
    this.sampleType = sampleType;
  }

  public int getNode_or_edge_type() {
    return node_or_edge_type;
  }

  public void setNode_or_edge_type(int node_or_edge_type) {
    this.node_or_edge_type = node_or_edge_type;
  }
}
