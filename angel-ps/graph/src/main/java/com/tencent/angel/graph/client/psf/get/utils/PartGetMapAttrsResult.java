package com.tencent.angel.graph.client.psf.get.utils;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.HashMap;
import java.util.Map;

public class PartGetMapAttrsResult extends PartitionGetResult {

  private int partId;
  private Long2ObjectOpenHashMap<Map<Integer, float[]>> nodeIdToContents;

  public PartGetMapAttrsResult(int partId,
      Long2ObjectOpenHashMap<Map<Integer, float[]>> nodeIdToContents) {
    this.partId = partId;
    this.nodeIdToContents = nodeIdToContents;
  }

  public PartGetMapAttrsResult() {
    this(-1, null);
  }

  public int getPartId() {
    return partId;
  }

  public Long2ObjectOpenHashMap<Map<Integer, float[]>> getNodeIdToContents() {
    return nodeIdToContents;
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeInt(output, partId);
    ByteBufSerdeUtils.serializeInt(output, nodeIdToContents.size());
    for (Long2ObjectOpenHashMap.Entry<Map<Integer, float[]>> entry : nodeIdToContents.long2ObjectEntrySet()) {
      ByteBufSerdeUtils.serializeLong(output, entry.getLongKey());
      Map<Integer, float[]> values = entry.getValue();
      if (values != null) {
        ByteBufSerdeUtils.serializeInt(output, values.size());
        for(Map.Entry<Integer, float[]> e: values.entrySet()) {
          ByteBufSerdeUtils.serializeInt(output, e.getKey());
          ByteBufSerdeUtils.serializeFloats(output, e.getValue());
        }
      } else {
        ByteBufSerdeUtils.serializeInt(output, 0);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = ByteBufSerdeUtils.deserializeInt(input);
    int size = ByteBufSerdeUtils.deserializeInt(input);
    nodeIdToContents = new Long2ObjectOpenHashMap<>(size);
    for (int i = 0; i < size; i++) {
      long key = ByteBufSerdeUtils.deserializeLong(input);
      int len = ByteBufSerdeUtils.deserializeInt(input);
      if (len > 0) {
        Map<Integer, float[]> e = new HashMap<>(len);
        for (int j = 0; j < len; j ++) {
          e.put(ByteBufSerdeUtils.deserializeInt(input), ByteBufSerdeUtils.deserializeFloats(input));
        }
        nodeIdToContents.put(key, e);
      }
    }
  }

  @Override
  public int bufferLen() {
    int len = 2 * ByteBufSerdeUtils.INT_LENGTH;
    for (Long2ObjectOpenHashMap.Entry<Map<Integer, float[]>> entry : nodeIdToContents.long2ObjectEntrySet()) {
      len += ByteBufSerdeUtils.LONG_LENGTH + ByteBufSerdeUtils.INT_LENGTH;
      if (entry.getValue() != null) {
        for(Map.Entry<Integer, float[]> e: entry.getValue().entrySet()) {
          len += ByteBufSerdeUtils.INT_LENGTH;
          len += ByteBufSerdeUtils.serializedFloatsLen(e.getValue());
        }
      }
    }
    return len;
  }
}
