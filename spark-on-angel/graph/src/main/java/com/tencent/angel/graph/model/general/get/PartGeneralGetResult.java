package com.tencent.angel.graph.model.general.get;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

public class PartGeneralGetResult extends PartitionGetResult {

  private transient Class<? extends IElement> dataClass;
  private long[] nodeIds;
  private IElement[] data;

  public PartGeneralGetResult(Class<? extends IElement> dataClass, long[] nodeIds, IElement[] data) {
    this.dataClass = dataClass;
    this.nodeIds = nodeIds;
    this.data = data;
  }

  public PartGeneralGetResult() {

  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeLongs(output, nodeIds);
    ByteBufSerdeUtils.serializeObjects(output, dataClass, data);
  }

  @Override
  public void deserialize(ByteBuf input) {
    nodeIds = ByteBufSerdeUtils.deserializeLongs(input);
    data = ByteBufSerdeUtils.deserializeObjects(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedLongsLen(nodeIds) + ByteBufSerdeUtils
        .serializedObjectsLen(dataClass, data);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public void setNodeIds(long[] nodeIds) {
    this.nodeIds = nodeIds;
  }

  public IElement[] getData() {
    return data;
  }

  public void setData(IElement[] data) {
    this.data = data;
  }
}
