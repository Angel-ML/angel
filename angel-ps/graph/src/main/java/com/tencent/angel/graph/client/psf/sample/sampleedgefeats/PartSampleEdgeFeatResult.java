package com.tencent.angel.graph.client.psf.sample.sampleedgefeats;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.client.psf.sample.sampleneighbor.PartSampleNeighborResult;
import com.tencent.angel.graph.data.Feature;
import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.graph.data.NeighborStorageType;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class PartSampleEdgeFeatResult extends PartSampleNeighborResult {

  /**
   * Node id to sample edge feat map
   */
  public Long2ObjectOpenHashMap<Feature[]> nodeIdToSampleEdgeFeats;

  public PartSampleEdgeFeatResult(int partId,
      Long2ObjectOpenHashMap<Neighbor> nodeIdToSampleNeighbors,
      Long2ObjectOpenHashMap<Feature[]> nodeIdToSampleEdgeFeats, NeighborStorageType storageType) {
    super(partId, nodeIdToSampleNeighbors, storageType);
    this.nodeIdToSampleEdgeFeats = nodeIdToSampleEdgeFeats;
  }

  public PartSampleEdgeFeatResult() {
    this(-1, null, null, NeighborStorageType.LONGARRAY);
  }

  public Long2ObjectOpenHashMap<Neighbor> getNodeIdToSampleNeighbors() {
    return nodeIdToSampleNeighbors;
  }

  public void setNodeIdToSampleNeighbors(
      Long2ObjectOpenHashMap<Neighbor> nodeIdToSampleNeighbors) {
    this.nodeIdToSampleNeighbors = nodeIdToSampleNeighbors;
  }

  public Long2ObjectOpenHashMap<Feature[]> getNodeIdToSampleEdgeFeats() {
    return nodeIdToSampleEdgeFeats;
  }

  public void setNodeIdToSampleEdgeFeats(
      Long2ObjectOpenHashMap<Feature[]> nodeIdToSampleEdgeFeats) {
    this.nodeIdToSampleEdgeFeats = nodeIdToSampleEdgeFeats;
  }

  public int getPartId() {
    return partId;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);

    if (nodeIdToSampleEdgeFeats != null) {
      ByteBufSerdeUtils.serializeInt(output, nodeIdToSampleEdgeFeats.size());

      for (Long2ObjectOpenHashMap.Entry<Feature[]> entry : nodeIdToSampleEdgeFeats
          .long2ObjectEntrySet()) {
        ByteBufSerdeUtils.serializeLong(output, entry.getLongKey());
        Feature[] sampleEdgeFeats = entry.getValue();
        ByteBufSerdeUtils.serializeInt(output, sampleEdgeFeats.length);
        for (Feature edgeFeats : sampleEdgeFeats) {
          edgeFeats.serialize(output);
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
      nodeIdToSampleEdgeFeats = new Long2ObjectOpenHashMap<>(nodeTypeSize);
      for (int i = 0; i < nodeTypeSize; i++) {
        long nodeId = ByteBufSerdeUtils.deserializeLong(input);
        int len = ByteBufSerdeUtils.deserializeInt(input);
        Feature[] edgeFeats = new Feature[len];
        for (int j = 0; j < len; j++) {
          edgeFeats[j] = new Feature();
          edgeFeats[j].deserialize(input);
        }
        nodeIdToSampleEdgeFeats.put(nodeId, edgeFeats);
      }
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();

    len += ByteBufSerdeUtils.INT_LENGTH;
    if (nodeIdToSampleEdgeFeats != null) {
      for (Long2ObjectOpenHashMap.Entry<Feature[]> entry : nodeIdToSampleEdgeFeats
          .long2ObjectEntrySet()) {
        len += ByteBufSerdeUtils.LONG_LENGTH;
        len += ByteBufSerdeUtils.INT_LENGTH;
        for (Feature edgeFeats : entry.getValue()) {
          len += edgeFeats.bufferLen();
        }
      }
    }

    return len;
  }
}
