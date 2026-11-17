package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.client.constent.Constent;
import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.graph.data.NeighborStorageType;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

/**
 * Result of GetNeighbor with type type: node type or edge type
 */
public class PartSampleNeighborResultWithType extends PartSampleNeighborResult {

  /**
   * Node id to neighbors type map
   */
  private Long2ObjectOpenHashMap<int[]> nodeIdToSampleNeighborsType;

  /**
   * Node id to edge type map
   */
  private Long2ObjectOpenHashMap<int[]> nodeIdToSampleEdgeType;


  public PartSampleNeighborResultWithType(int partId,
      Long2ObjectOpenHashMap<Neighbor> nodeIdToSampleNeighbors,
      Long2ObjectOpenHashMap<int[]> nodeIdToType, SampleType sampleType,
      NeighborStorageType storageType) {
    super(partId, nodeIdToSampleNeighbors, storageType);
    if (sampleType == SampleType.NODE) {
      this.nodeIdToSampleNeighborsType = nodeIdToType;
    } else if (sampleType == SampleType.EDGE) {
      this.nodeIdToSampleEdgeType = nodeIdToType;
    }
  }

  public PartSampleNeighborResultWithType(int partId,
      Long2ObjectOpenHashMap<Neighbor> nodeIdToSampleNeighbors,
      Long2ObjectOpenHashMap<int[]> nodeIdToSampleNeighborsType,
      Long2ObjectOpenHashMap<int[]> nodeIdToSampleEdgeType, NeighborStorageType storageType) {
    super(partId, nodeIdToSampleNeighbors, storageType);
    this.nodeIdToSampleNeighborsType = nodeIdToSampleNeighborsType;
    this.nodeIdToSampleEdgeType = nodeIdToSampleEdgeType;
  }

  public PartSampleNeighborResultWithType() {
    this(-1, null, null, SampleType.SIMPLE, NeighborStorageType.LONGARRAY);
  }

  public Long2ObjectOpenHashMap<int[]> getNodeIdToSampleNeighborsType() {
    return nodeIdToSampleNeighborsType;
  }

  public void setNodeIdToSampleNeighborsType(
      Long2ObjectOpenHashMap<int[]> nodeIdToSampleNeighborsType) {
    this.nodeIdToSampleNeighborsType = nodeIdToSampleNeighborsType;
  }

  public Long2ObjectOpenHashMap<int[]> getNodeIdToSampleEdgeType() {
    return nodeIdToSampleEdgeType;
  }

  public void setNodeIdToSampleEdgeType(
      Long2ObjectOpenHashMap<int[]> nodeIdToSampleEdgeType) {
    this.nodeIdToSampleEdgeType = nodeIdToSampleEdgeType;
  }

  public int getPartId() {
    return partId;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);

    if (nodeIdToSampleNeighborsType != null) {
      ByteBufSerdeUtils.serializeInt(output, nodeIdToSampleNeighborsType.size());

      for (Long2ObjectOpenHashMap.Entry<int[]> entry : nodeIdToSampleNeighborsType
          .long2ObjectEntrySet()) {
        ByteBufSerdeUtils.serializeLong(output, entry.getLongKey());
        int[] sampleNeighborsType = entry.getValue();
        if (sampleNeighborsType == null) {
          ByteBufSerdeUtils.serializeInts(output, Constent.emptyInts);
        } else {
          ByteBufSerdeUtils.serializeInts(output, sampleNeighborsType);
        }
      }
    } else {
      ByteBufSerdeUtils.serializeInt(output, 0);
    }

    if (nodeIdToSampleEdgeType != null) {
      ByteBufSerdeUtils.serializeInt(output, nodeIdToSampleEdgeType.size());
      for (Long2ObjectOpenHashMap.Entry<int[]> entry : nodeIdToSampleEdgeType
          .long2ObjectEntrySet()) {
        ByteBufSerdeUtils.serializeLong(output, entry.getLongKey());
        int[] sampleEdgeType = entry.getValue();
        if (sampleEdgeType == null) {
          ByteBufSerdeUtils.serializeInts(output, Constent.emptyInts);
        } else {
          ByteBufSerdeUtils.serializeInts(output, sampleEdgeType);
        }
      }
    } else {
      ByteBufSerdeUtils.serializeInt(output, 0);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);

    int nodeTypeSize = ByteBufSerdeUtils.deserializeInt(input);
    if (nodeTypeSize > 0) {
      nodeIdToSampleNeighborsType = new Long2ObjectOpenHashMap<>(nodeTypeSize);
      for (int i = 0; i < nodeTypeSize; i++) {
        long nodeId = ByteBufSerdeUtils.deserializeLong(input);
        int[] sampleNeighborsType = ByteBufSerdeUtils.deserializeInts(input);
        nodeIdToSampleNeighborsType.put(nodeId, sampleNeighborsType);
      }
    }

    int nodeEdgeSize = ByteBufSerdeUtils.deserializeInt(input);
    if (nodeEdgeSize > 0) {
      nodeIdToSampleEdgeType = new Long2ObjectOpenHashMap<>(nodeEdgeSize);
      for (int i = 0; i < nodeEdgeSize; i++) {
        long nodeId = ByteBufSerdeUtils.deserializeLong(input);
        int[] sampleEdgeType = ByteBufSerdeUtils.deserializeInts(input);
        nodeIdToSampleEdgeType.put(nodeId, sampleEdgeType);
      }
    }

  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();

    len += ByteBufSerdeUtils.INT_LENGTH;
    if (nodeIdToSampleNeighborsType != null) {
      for (Long2ObjectOpenHashMap.Entry<int[]> entry : nodeIdToSampleNeighborsType
          .long2ObjectEntrySet()) {
        len += ByteBufSerdeUtils.LONG_LENGTH;
        int[] sampleNeighborsType = entry.getValue();
        if (sampleNeighborsType == null) {
          len += ByteBufSerdeUtils.serializedIntsLen(Constent.emptyInts);
        } else {
          len += ByteBufSerdeUtils.serializedIntsLen(sampleNeighborsType);
        }
      }
    }

    len += ByteBufSerdeUtils.INT_LENGTH;
    if (nodeIdToSampleEdgeType != null) {
      for (Long2ObjectOpenHashMap.Entry<int[]> entry : nodeIdToSampleEdgeType
          .long2ObjectEntrySet()) {
        len += ByteBufSerdeUtils.LONG_LENGTH;
        int[] sampleEdgeType = entry.getValue();
        if (sampleEdgeType == null) {
          len += ByteBufSerdeUtils.serializedIntsLen(Constent.emptyInts);
        } else {
          len += ByteBufSerdeUtils.serializedIntsLen(sampleEdgeType);
        }
      }
    }
    return len;
  }
}
